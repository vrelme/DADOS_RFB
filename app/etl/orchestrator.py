# app/etl/orchestrator.py

import time
import socket
import logging
import threading
import traceback
import zipfile

import psutil
import numpy as np
import pandas as pd

from pathlib import Path

from concurrent.futures import (
    ProcessPoolExecutor,
    as_completed
)
from concurrent.futures.process import BrokenProcessPool

from app.config import Settings

from app.database import SessionLocal

from app.models import (
    Empresa,
    EmpresaStaging,

    Estabelecimento,
    EstabelecimentoStaging,

    Socio,
    SocioStaging
)

from app.etl.transformer import DataTransformer
from app.etl.validator import Validator
from app.etl.deduplicator import Deduplicator
from app.etl.bulk_repository import BulkRepository

from app.etl.etl_execution_repository import (
    ETLExecutionRepository
)
from app.etl.observability_repository import ObservabilityRepository

logger = logging.getLogger(__name__)


# =====================================================
# WORKER TASK
# =====================================================
def process_file_task(args):

    if len(args) == 6:
        (
            file_path,
            staging_model,
            columns,
            key,
            table_name,
            run_id,
        ) = args
    else:
        (
            file_path,
            staging_model,
            columns,
            key,
            table_name
        ) = args
        run_id = None

    orchestrator = ETLOrchestrator(run_id=run_id)

    try:
        orchestrator._execute_pipeline(
            file_path=file_path,
            staging_model=staging_model,
            columns=columns,
            key=key,
            table_name=table_name
        )
    except Exception as exc:
        details = traceback.format_exc()
        raise RuntimeError(
            f"Worker falhou ao processar {Path(file_path).name}: {exc}\n{details}"
        ) from None


# =====================================================
# ORCHESTRATOR
# =====================================================
class ETLOrchestrator:

    def __init__(self, logger_instance=None, run_id=None):

        self.logger = logger_instance or logger

        self.run_id = run_id

        self.transformer = DataTransformer()

        self.validator = Validator()

        self.deduplicator = Deduplicator()

        self.chunk_size = Settings.CHUNK_SIZE

    # =====================================================
    # HEARTBEAT
    # =====================================================
    def _heartbeat(self):  # <- incluída no código

        while True:

            self.logger.info(
                "HEARTBEAT     | ETL em execução..."
            )

            if self.run_id:
                db = SessionLocal()
                try:
                    ObservabilityRepository(db).heartbeat_run(self.run_id)
                finally:
                    db.close()

            time.sleep(60)

    # =====================================================
    # SYSTEM MONITOR
    # =====================================================
    def _system_monitor(self):  # <- incluída no código

        while True:

            cpu = psutil.cpu_percent()

            ram = psutil.virtual_memory().percent

            self.logger.info(
                f"MONITOR        | CPU={cpu}% | RAM={ram}%"
            )

            time.sleep(60)

    # =====================================================
    # MAIN
    # =====================================================
    def run(self):

        self.logger.info("=" * 80)
        self.logger.info("RFB LOADER ENTERPRISE")
        self.logger.info("=" * 80)

        Settings.create_dirs()

        if not self.run_id:
            self.run_id = self._start_run()

        # =============================================
        # HEARTBEAT THREAD
        # =============================================
        threading.Thread(                 # <- incluída no código
            target=self._heartbeat,      # <- incluída no código
            daemon=True                  # <- incluída no código
        ).start()

        # =============================================
        # SYSTEM MONITOR THREAD
        # =============================================
        threading.Thread(                    # <- incluída no código
            target=self._system_monitor,    # <- incluída no código
            daemon=True                     # <- incluída no código
        ).start()

        try:
            self._process_empresa()

            self._process_estabelecimento()

            self._process_socio()

            self._finish_run("SUCCESS")

            self.logger.info("=" * 80)
            self.logger.info("PIPELINE FINALIZADO")
            self.logger.info("=" * 80)

        except Exception as exc:
            self._finish_run("FAILED", str(exc))
            raise

    def _all_input_files_count(self):
        patterns = ["*.EMPRECSV", "*.ESTABELE", "*.SOCIOCSV"]
        total = 0
        for pattern in patterns:
            total += len(self._discover_files(pattern))
        return total

    def _start_run(self):
        db = SessionLocal()
        try:
            repo = ObservabilityRepository(db)
            run = repo.start_run(
                pipeline="RFB_LOADER_ENTERPRISE",
                sync_strategy=Settings.SYNC_STRATEGY,
                load_target=Settings.LOAD_TARGET,
                active_database=Settings.ACTIVE_DB_NAME,
                total_files=self._all_input_files_count(),
            )
            self.logger.info(
                f"ETL RUN START | id={run.id} | arquivos={run.total_files} | "
                f"db={Settings.ACTIVE_DB_NAME}"
            )
            return run.id
        finally:
            db.close()

    def _finish_run(self, status, error_message=None):
        if not self.run_id:
            return
        db = SessionLocal()
        try:
            ObservabilityRepository(db).finish_run(
                self.run_id,
                status=status,
                error_message=error_message,
            )
            self.logger.info(f"ETL RUN {status} | id={self.run_id}")
        finally:
            db.close()

    def _start_phase(self, phase_name, table_name=None, message=None):
        if not self.run_id:
            return None
        db = SessionLocal()
        try:
            return ObservabilityRepository(db).start_phase(
                self.run_id,
                phase_name,
                table_name=table_name,
                message=message,
            )
        finally:
            db.close()

    def _finish_phase(self, phase, status="SUCCESS", message=None):
        if not phase:
            return
        db = SessionLocal()
        try:
            ObservabilityRepository(db).finish_phase(
                phase,
                status=status,
                message=message,
            )
        finally:
            db.close()

    def _update_run_context(self, phase=None, table_name=None, file_name=None):
        if not self.run_id:
            return
        db = SessionLocal()
        try:
            ObservabilityRepository(db).update_run_context(
                self.run_id,
                phase=phase,
                table_name=table_name,
                file_name=file_name,
            )
        finally:
            db.close()

    # =====================================================
    # PARALELISMO
    # =====================================================
    def _run_parallel(self, tasks):

        if not tasks:

            self.logger.warning(
                "Nenhum arquivo encontrado"
            )

            return

        if Settings.ENABLE_ADAPTIVE_WORKERS:
            return self._run_parallel_adaptive(tasks)

        return self._run_parallel_batch(tasks, Settings.MAX_WORKERS)

    def _run_parallel_adaptive(self, tasks):

        pending = list(tasks)
        completed = 0
        start_parallel = time.time()

        self.logger.info(f"{len(tasks)} arquivos encontrados")

        while pending:
            workers = self._calculate_worker_count()
            wave = pending[:workers]
            pending = pending[workers:]

            self.logger.info(
                f"Workers adaptativos: {workers} | pendentes={len(pending)}"
            )

            self._run_parallel_batch(wave, workers, log_summary=False)
            completed += len(wave)

        elapsed = round(time.time() - start_parallel, 2)
        self.logger.info("-" * 80)
        self.logger.info(
            f"PARALELISMO ADAPTATIVO FINALIZADO | "
            f"arquivos={completed} | {elapsed}s"
        )

    def _calculate_worker_count(self):

        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        workers = Settings.MAX_WORKERS

        if cpu >= Settings.ADAPTIVE_CPU_HIGH or ram >= Settings.ADAPTIVE_RAM_HIGH:
            workers = max(Settings.MIN_WORKERS, Settings.MAX_WORKERS - 1)
        elif cpu <= Settings.ADAPTIVE_CPU_LOW and ram <= Settings.ADAPTIVE_RAM_LOW:
            workers = Settings.MAX_WORKERS

        return max(Settings.MIN_WORKERS, min(workers, Settings.MAX_WORKERS))

    def _run_parallel_batch(self, tasks, workers, log_summary=True):

        self.logger.info(f"Workers: {workers}")

        start_parallel = time.time()

        try:
            with ProcessPoolExecutor(max_workers=workers) as executor:

                futures = [
                    executor.submit(process_file_task, task)
                    for task in tasks
                ]

                for future in as_completed(futures):

                    try:
                        future.result()

                    except BrokenProcessPool as e:
                        self.logger.error(
                            "Pool de workers encerrado inesperadamente. "
                            "Em Windows isso normalmente indica erro não serializável "
                            "ou falha fatal dentro do worker.",
                            exc_info=True
                        )
                        raise RuntimeError(
                            "Falha no processamento paralelo. "
                            "Execute temporariamente ENABLE_PARALLELISM=False para isolar o arquivo."
                        ) from e

                    except Exception as e:
                        self.logger.error(
                            f"Erro worker paralelo: {e}",
                            exc_info=True
                        )
                        raise

        finally:
            if log_summary:
                elapsed = round(time.time() - start_parallel, 2)
                self.logger.info("-" * 80)
                self.logger.info(
                    f"PARALELISMO FINALIZADO em {elapsed}s"
                )

    def _load_files_to_staging(self, files, table_name):

        if not files:
            self.logger.warning(
                f"Nenhum arquivo encontrado para {table_name}"
            )
            return

        if (
            Settings.LOAD_STRATEGY == "load_data"
            and Settings.ENABLE_PARALLELISM
        ):
            tasks = [
                (file, None, None, None, table_name, self.run_id)
                for file in files
            ]
            self._run_parallel(tasks)
            return

        if Settings.LOAD_STRATEGY == "load_data":
            for file_path in files:
                self._execute_pipeline(
                    file_path=file_path,
                    staging_model=None,
                    columns=None,
                    key=None,
                    table_name=table_name
                )

    def _extract_zip_files(self):

        if not Settings.ENABLE_ZIP_PROCESSING:
            return

        zip_files = list(Path(Settings.INPUT_DIR).glob("*.zip"))

        for zip_path in zip_files:
            target_dir = Path(Settings.EXTRACT_DIR) / zip_path.stem
            target_dir.mkdir(parents=True, exist_ok=True)

            try:
                with zipfile.ZipFile(zip_path) as archive:
                    archive.extractall(target_dir)

                self.logger.info(
                    f"ZIP extraído | {zip_path.name} -> {target_dir}"
                )

            except zipfile.BadZipFile:
                self.logger.error(f"ZIP inválido: {zip_path}")
                raise

    def _discover_files(self, pattern):

        self._extract_zip_files()

        files = list(Path(Settings.INPUT_DIR).glob(pattern))

        if Settings.ENABLE_ZIP_PROCESSING:
            files.extend(Path(Settings.EXTRACT_DIR).rglob(pattern))

        return sorted(set(files))

    def _target_table_for_load(self, table_name):
        if Settings.LOAD_TARGET == "final":
            return table_name
        return f"{table_name}_staging"

    def _should_merge_after_load(self):
        return Settings.LOAD_TARGET != "final"

    def _should_truncate_before_load(self):
        return not (
            Settings.SYNC_STRATEGY in Settings.RAW_IMPORT_STRATEGIES
            and Settings.LOAD_TARGET == "final"
        )

    def _truncate_target_if_needed(self, repo, table_name):
        target_table = self._target_table_for_load(table_name)

        if not self._should_truncate_before_load():
            self.logger.info(
                f"RAW_IMPORT banco por execução ativo: TRUNCATE {target_table} ignorado | "
                f"db={Settings.ACTIVE_DB_NAME}"
            )
            return

        truncate_start = time.time()
        repo.truncate_table(target_table)
        self.logger.info(
            f"TRUNCATE {target_table} concluído em "
            f"{round(time.time()-truncate_start,2)}s"
        )

    # =====================================================
    # EMPRESA
    # =====================================================
    def _process_empresa(self):

        self.logger.info("-" * 80)
        self.logger.info(
            "PROCESSANDO EMPRESA"
        )
        self.logger.info("-" * 80)

        phase = self._start_phase("LOAD_EMPRESA", table_name="empresa")

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            self._truncate_target_if_needed(repo, "empresa")

        finally:

            db.close()

        files = self._discover_files("*.EMPRECSV")

        columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo"
        ]

        tasks = [

            (
                file,
                EmpresaStaging,
                columns,
                ["cnpj_basico"],
                "empresa",
                self.run_id
            )

            for file in files
        ]

        # =============================================
        # WORKERS -> STAGING
        # =============================================
        if Settings.LOAD_STRATEGY == "load_data":
            self._load_files_to_staging(files, "empresa")
        else:
            self._run_parallel(tasks)

        # =============================================
        # MERGE CENTRALIZADO
        # =============================================
        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            merge_start = time.time()

            if self._should_merge_after_load():
                repo.merge_empresa()

                self.logger.info(
                    f"MERGE empresa concluído "
                    f"em {round(time.time()-merge_start,2)}s"
                )
            else:
                self.logger.info("LOAD_TARGET=final: merge empresa ignorado")

        finally:

            db.close()
            self._finish_phase(phase)

    # =====================================================
    # ESTABELECIMENTO
    # =====================================================
    def _process_estabelecimento(self):

        self.logger.info("-" * 80)
        self.logger.info(
            "PROCESSANDO ESTABELECIMENTO"
        )
        self.logger.info("-" * 80)

        phase = self._start_phase("LOAD_ESTABELECIMENTO", table_name="estabelecimento")

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            self._truncate_target_if_needed(repo, "estabelecimento")

        finally:

            db.close()

        files = self._discover_files("*.ESTABELE")

        columns = [
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "identificador_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "ddd1",
            "telefone1",
            "ddd2",
            "telefone2",
            "ddd_fax",
            "fax",
            "email",
            "situacao_especial",
            "data_situacao_especial"
        ]

        tasks = [

            (
                file,
                EstabelecimentoStaging,
                columns,
                [
                    "cnpj_basico",
                    "cnpj_ordem",
                    "cnpj_dv"
                ],
                "estabelecimento",
                self.run_id
            )

            for file in files
        ]

        if Settings.LOAD_STRATEGY == "load_data":
            self._load_files_to_staging(files, "estabelecimento")
        else:
            self._run_parallel(tasks)

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            merge_start = time.time()

            if self._should_merge_after_load():
                repo.merge_estabelecimento()

                self.logger.info(
                    f"MERGE estabelecimento "
                    f"concluído em "
                    f"{round(time.time()-merge_start,2)}s"
                )
            else:
                self.logger.info("LOAD_TARGET=final: merge estabelecimento ignorado")

        finally:

            db.close()
            self._finish_phase(phase)

    # =====================================================
    # SOCIO
    # =====================================================
    def _process_socio(self):

        self.logger.info("-" * 80)
        self.logger.info(
            "PROCESSANDO SOCIO"
        )
        self.logger.info("-" * 80)

        phase = self._start_phase("LOAD_SOCIO", table_name="socio")

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            self._truncate_target_if_needed(repo, "socio")

        finally:

            db.close()

        files = self._discover_files("*.SOCIOCSV")

        columns = [
            "cnpj_basico",
            "identificador_socio",
            "nome_socio",
            "cpf_cnpj_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "representante_legal",
            "nome_representante",
            "qualificacao_representante_legal",
            "faixa_etaria"
        ]

        tasks = [

            (
                file,
                SocioStaging,
                columns,
                ["cnpj_basico"],
                "socio",
                self.run_id
            )

            for file in files
        ]

        if Settings.LOAD_STRATEGY == "load_data":
            self._load_files_to_staging(files, "socio")
        else:
            self._run_parallel(tasks)

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            merge_start = time.time()

            if self._should_merge_after_load():
                repo.merge_socio()

                self.logger.info(
                    f"MERGE socio concluído "
                    f"em {round(time.time()-merge_start,2)}s"
                )
            else:
                self.logger.info("LOAD_TARGET=final: merge socio ignorado")

        finally:

            db.close()
            self._finish_phase(phase)

    # =====================================================
    # PIPELINE
    # =====================================================
    def _execute_pipeline(
        self,
        file_path,
        staging_model,
        columns,
        key,
        table_name
    ):

        db = SessionLocal()

        repo = BulkRepository(db)

        execution_repo = ETLExecutionRepository(db)

        observability_repo = ObservabilityRepository(db)

        execution = None

        file_progress = None

        total = 0

        start_time = time.time()

        try:

            self.logger.info("-" * 80)
            self.logger.info(
                f"PROCESSANDO: {file_path.name}"
            )
            self.logger.info("-" * 80)

            if (
                Settings.ENABLE_CHECKPOINT_RESUME
                and observability_repo.already_successful(
                    "RFB_LOADER_ENTERPRISE",
                    table_name,
                    file_path.name,
                )
            ):
                self.logger.info(
                    f"CHECKPOINT | arquivo já processado com sucesso: {file_path.name}"
                )
                if self.run_id:
                    skipped_progress = observability_repo.start_file_progress(
                        self.run_id,
                        "RFB_LOADER_ENTERPRISE",
                        table_name,
                        file_path.name,
                    )
                    observability_repo.finish_file_progress(
                        skipped_progress,
                        self.run_id,
                        status="SKIPPED",
                        records_processed=0,
                    )
                return

            observability_repo.checkpoint(
                pipeline="RFB_LOADER_ENTERPRISE",
                table_name=table_name,
                file_name=file_path.name,
                status="STARTED",
            )

            if self.run_id:
                file_progress = observability_repo.start_file_progress(
                    self.run_id,
                    "RFB_LOADER_ENTERPRISE",
                    table_name,
                    file_path.name,
                )

            # =============================================
            # EXECUTION START
            # =============================================
            execution = execution_repo.start(
                pipeline="RFB_LOADER_ENTERPRISE",
                file_name=file_path.name,
                table_name=table_name,
                worker=socket.gethostname(),
                environment=Settings.ENVIRONMENT
            )

            if Settings.LOAD_STRATEGY == "load_data":
                total = repo.load_file_to_staging(
                    table_name=table_name,
                    file_path=file_path
                )
                execution_repo.success(
                    execution,
                    total
                )
                total_time = round(
                    time.time() - start_time,
                    2
                )
                observability_repo.checkpoint(
                    pipeline="RFB_LOADER_ENTERPRISE",
                    table_name=table_name,
                    file_name=file_path.name,
                    status="SUCCESS",
                    records_processed=total,
                )
                observability_repo.metric(
                    pipeline="RFB_LOADER_ENTERPRISE",
                    metric_name="records_processed",
                    metric_value=total,
                    unit="rows",
                    table_name=table_name,
                    file_name=file_path.name,
                    worker=socket.gethostname(),
                )
                observability_repo.metric(
                    pipeline="RFB_LOADER_ENTERPRISE",
                    metric_name="execution_seconds",
                    metric_value=total_time,
                    unit="seconds",
                    table_name=table_name,
                    file_name=file_path.name,
                    worker=socket.gethostname(),
                )
                if self.run_id:
                    observability_repo.finish_file_progress(
                        file_progress,
                        self.run_id,
                        status="SUCCESS",
                        records_processed=total,
                    )
                self.logger.info(
                    f"FINALIZADO | "
                    f"{file_path.name} | "
                    f"{total} registros | "
                    f"{total_time}s"
                )
                return

            # =============================================
            # READ CSV
            # =============================================
            for chunk in pd.read_csv(
                file_path,
                sep=Settings.FILE_SEPARATOR,
                names=columns,
                dtype=str,
                chunksize=self.chunk_size,
                encoding=Settings.FILE_ENCODING
            ):

                chunk_start = time.time()

                # =========================================
                # TRANSFORM
                # =========================================
                chunk = self.transformer.sanitize(
                    chunk
                )

                # =========================================
                # VALIDATE
                # =========================================
                chunk = self._validate(
                    table_name,
                    chunk
                )

                # =========================================
                # DEDUPLICAÇÃO
                # =========================================
                chunk = self.deduplicator.drop_duplicates(
                    chunk,
                    key
                )

                # =========================================
                # NaN -> None
                # =========================================
                chunk = chunk.replace(
                    {np.nan: None}
                )

                data = chunk.to_dict(
                    orient="records"
                )

                if not data:

                    continue

                # =========================================
                # INSERT STAGING
                # =========================================
                repo.bulk_insert(
                    staging_model,
                    data
                )

                total += len(data)

                # =========================================
                # PERFORMANCE LOG
                # =========================================
                elapsed = (
                    time.time() - chunk_start
                )

                rps = (
                    int(len(data) / elapsed)
                    if elapsed > 0
                    else 0
                )

                observability_repo.metric(
                    pipeline="RFB_LOADER_ENTERPRISE",
                    metric_name="chunk_records_processed",
                    metric_value=len(data),
                    unit="rows",
                    table_name=table_name,
                    file_name=file_path.name,
                    worker=socket.gethostname(),
                )

                self.logger.info(
                    f"{file_path.name} | "
                    f"+{len(data)} | "
                    f"{rps} reg/s | "
                    f"total={total}"
                )

            # =============================================
            # SUCCESS
            # =============================================
            execution_repo.success(
                execution,
                total
            )
            observability_repo.checkpoint(
                pipeline="RFB_LOADER_ENTERPRISE",
                table_name=table_name,
                file_name=file_path.name,
                status="SUCCESS",
                records_processed=total,
            )
            if self.run_id:
                observability_repo.finish_file_progress(
                    file_progress,
                    self.run_id,
                    status="SUCCESS",
                    records_processed=total,
                )

            total_time = round(
                time.time() - start_time,
                2
            )

            self.logger.info(
                f"FINALIZADO | "
                f"{file_path.name} | "
                f"{total} registros | "
                f"{total_time}s"
            )

        except Exception as e:

            self.logger.error(
                f"Erro crítico pipeline "
                f"{file_path.name}: {e}",
                exc_info=True
            )

            observability_repo.checkpoint(
                pipeline="RFB_LOADER_ENTERPRISE",
                table_name=table_name,
                file_name=file_path.name,
                status="FAILED",
                records_processed=total,
                error_message=str(e),
            )
            if self.run_id:
                observability_repo.finish_file_progress(
                    file_progress,
                    self.run_id,
                    status="FAILED",
                    records_processed=total,
                    error_message=str(e),
                )

            if execution:

                execution_repo.failed(
                    execution,
                    str(e)
                )

            raise

        finally:

            db.close()

    # =====================================================
    # VALIDATION ROUTER
    # =====================================================
    def _validate(
        self,
        table_name,
        df
    ):

        if table_name == "empresa":

            return self.validator.validate_empresa(df)

        elif table_name == "estabelecimento":

            return self.validator.validate_estabelecimento(df)

        elif table_name == "socio":

            return self.validator.validate_socio(df)

        return df
