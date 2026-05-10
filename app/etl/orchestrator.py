# app/etl/orchestrator.py

import time
import socket
import logging
import threading
import traceback

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

logger = logging.getLogger(__name__)


# =====================================================
# WORKER TASK
# =====================================================
def process_file_task(args):

    (
        file_path,
        staging_model,
        columns,
        key,
        table_name
    ) = args

    orchestrator = ETLOrchestrator()

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

    def __init__(self, logger_instance=None):

        self.logger = logger_instance or logger

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
                "HEARTBEAT | ETL em execução..."
            )

            time.sleep(60)

    # =====================================================
    # SYSTEM MONITOR
    # =====================================================
    def _system_monitor(self):  # <- incluída no código

        while True:

            cpu = psutil.cpu_percent()

            ram = psutil.virtual_memory().percent

            self.logger.info(
                f"MONITOR   | CPU={cpu}% | RAM={ram}%"
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

        self._process_empresa()

        self._process_estabelecimento()

        self._process_socio()

        self.logger.info("=" * 80)
        self.logger.info("PIPELINE FINALIZADO")
        self.logger.info("=" * 80)

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
                (file, None, None, None, table_name)
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

    def _target_table_for_load(self, table_name):
        if Settings.LOAD_TARGET == "final":
            return table_name
        return f"{table_name}_staging"

    def _should_merge_after_load(self):
        return Settings.LOAD_TARGET != "final"

    # =====================================================
    # EMPRESA
    # =====================================================
    def _process_empresa(self):

        self.logger.info(
            "PROCESSANDO EMPRESA"
        )

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            truncate_start = time.time()  # <- incluída no código

            repo.truncate_table(
                self._target_table_for_load("empresa")
            )

            self.logger.info(  # <- incluída no código
                f"TRUNCATE {self._target_table_for_load("empresa")} "
                f"concluído em "
                f"{round(time.time()-truncate_start,2)}s"
            )

        finally:

            db.close()

        files = list(
            Path(Settings.INPUT_DIR)
            .glob("*.EMPRECSV")
        )

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
                "empresa"
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

    # =====================================================
    # ESTABELECIMENTO
    # =====================================================
    def _process_estabelecimento(self):

        self.logger.info(
            "PROCESSANDO ESTABELECIMENTO"
        )

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            truncate_start = time.time()

            repo.truncate_table(
                self._target_table_for_load("estabelecimento")
            )

            self.logger.info(
                f"TRUNCATE {self._target_table_for_load("estabelecimento")} "
                f"concluído em "
                f"{round(time.time()-truncate_start,2)}s"
            )

        finally:

            db.close()

        files = list(
            Path(Settings.INPUT_DIR)
            .glob("*.ESTABELE")
        )

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
                "estabelecimento"
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

    # =====================================================
    # SOCIO
    # =====================================================
    def _process_socio(self):

        self.logger.info(
            "PROCESSANDO SOCIO"
        )

        db = SessionLocal()

        try:

            repo = BulkRepository(db)

            truncate_start = time.time()

            repo.truncate_table(
                self._target_table_for_load("socio")
            )

            self.logger.info(
                f"TRUNCATE {self._target_table_for_load("socio")} "
                f"concluído em "
                f"{round(time.time()-truncate_start,2)}s"
            )

        finally:

            db.close()

        files = list(
            Path(Settings.INPUT_DIR)
            .glob("*.SOCIOCSV")
        )

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
                "socio"
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

        execution = None

        total = 0

        start_time = time.time()

        try:

            self.logger.info(
                f"PROCESSANDO: {file_path.name}"
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
