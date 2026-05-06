# app/etl/orchestrator.py

import time
import socket
import logging

import numpy as np
import pandas as pd

from pathlib import Path

from concurrent.futures import (
    ProcessPoolExecutor,
    as_completed
)

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
# WORKER PROCESS
# =====================================================
def process_file_task(args):

    (
        file_path,
        model,
        staging_model,
        columns,
        key
    ) = args

    orchestrator = ETLOrchestrator()

    orchestrator._execute_pipeline(
        file_path=file_path,
        model=model,
        staging_model=staging_model,
        columns=columns,
        key=key
    )


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
    # MAIN
    # =====================================================
    def run(self):

        self.logger.info("=" * 80)
        self.logger.info("RFB LOADER ENTERPRISE")
        self.logger.info("=" * 80)

        Settings.create_dirs()

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

        self.logger.info(
            f"{len(tasks)} arquivos encontrados"
        )

        self.logger.info(
            f"Workers: {Settings.MAX_WORKERS}"
        )

        with ProcessPoolExecutor(
            max_workers=Settings.MAX_WORKERS
        ) as executor:

            futures = [
                executor.submit(
                    process_file_task,
                    task
                )
                for task in tasks
            ]

            for future in as_completed(futures):

                try:

                    future.result()

                except Exception as e:

                    self.logger.error(
                        f"Erro worker paralelo: {e}",
                        exc_info=True
                    )

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

            repo.truncate_table(
                "empresa_staging"
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
                Empresa,
                EmpresaStaging,
                columns,
                ["cnpj_basico"]
            )

            for file in files
        ]

        self._run_parallel(tasks)

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

            repo.truncate_table(
                "estabelecimento_staging"
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
                Estabelecimento,
                EstabelecimentoStaging,
                columns,
                [
                    "cnpj_basico",
                    "cnpj_ordem",
                    "cnpj_dv"
                ]
            )

            for file in files
        ]

        self._run_parallel(tasks)

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

            repo.truncate_table(
                "socio_staging"
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
                Socio,
                SocioStaging,
                columns,
                ["cnpj_basico"]
            )

            for file in files
        ]

        self._run_parallel(tasks)

    # =====================================================
    # EXECUTE PIPELINE
    # =====================================================
    def _execute_pipeline(
        self,
        file_path,
        model,
        staging_model,
        columns,
        key
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
            # START EXECUTION
            # =============================================
            execution = execution_repo.start(
                pipeline="RFB_LOADER_ENTERPRISE",
                file_name=file_path.name,
                table_name=model.__tablename__,
                worker=socket.gethostname(),
                environment=Settings.ENVIRONMENT
            )

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
                    model,
                    chunk
                )

                # =========================================
                # DEDUPLICATE
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
                # PERFORMANCE
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
            # MERGE FINAL
            # =============================================
            if model.__tablename__ == "empresa":

                repo.merge_empresa()

            elif model.__tablename__ == "estabelecimento":

                repo.merge_estabelecimento()

            elif model.__tablename__ == "socio":

                repo.merge_socio()

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
    # VALIDATE ROUTER
    # =====================================================
    def _validate(
        self,
        model,
        df
    ):

        table = model.__tablename__

        if table == "empresa":

            return self.validator.validate_empresa(df)

        elif table == "estabelecimento":

            return self.validator.validate_estabelecimento(df)

        elif table == "socio":

            return self.validator.validate_socio(df)

        return df