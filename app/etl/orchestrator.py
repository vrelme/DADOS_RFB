import pandas as pd
import numpy as np
import time
import logging

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

from app.database import SessionLocal
from app.config import Settings
from app.models import Empresa, Estabelecimento, Socio

from app.etl.transformer import DataTransformer
from app.etl.validator import Validator
from app.etl.bulk_repository import BulkRepository
from app.etl.deduplicator import Deduplicator

logger = logging.getLogger(__name__)


# =====================================================
# FUNÇÃO GLOBAL PARA MULTIPROCESSING (OBRIGATÓRIO)
# =====================================================
def process_file_task(args):
    file_path, model, columns, key = args

    orchestrator = ETLOrchestrator()
    orchestrator._execute_pipeline(file_path, model, columns, key)


class ETLOrchestrator:

    def __init__(self, logger_instance=None):
        self.logger = logger_instance or logger

        self.transformer = DataTransformer()
        self.validator = Validator()
        self.deduplicator = Deduplicator()

        self.chunk_size = Settings.CHUNK_SIZE

    # =====================================================
    # ENTRYPOINT
    # =====================================================
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("INICIANDO PIPELINE ETL (PARALELO)")
        self.logger.info("=" * 60)

        self._process_empresa()
        self._process_estabelecimento()
        self._process_socio()

        self.logger.info("=" * 60)
        self.logger.info("PIPELINE FINALIZADO")
        self.logger.info("=" * 60)

    # =====================================================
    # PARALELISMO
    # =====================================================
    def _run_parallel(self, tasks):

        if not tasks:
            self.logger.warning("Nenhuma tarefa para processar")
            return

        self.logger.info(
            f"Executando {len(tasks)} arquivos com {Settings.MAX_WORKERS} workers"
        )

        with ProcessPoolExecutor(max_workers=Settings.MAX_WORKERS) as executor:

            futures = [executor.submit(process_file_task, t) for t in tasks]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(
                        f"Erro em execução paralela: {e}",
                        exc_info=True
                    )

    # =====================================================
    # EMPRESA
    # =====================================================
    def _process_empresa(self):
        files = list(Path(Settings.INPUT_DIR).glob("*.EMPRECSV"))

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
            (file, Empresa, columns, ["cnpj_basico"])
            for file in files
        ]

        self._run_parallel(tasks)

    # =====================================================
    # ESTABELECIMENTO
    # =====================================================
    def _process_estabelecimento(self):
        files = list(Path(Settings.INPUT_DIR).glob("*.ESTABELE"))

        columns = [
            "cnpj_basico", "cnpj_ordem", "cnpj_dv",
            "identificador_matriz_filial", "nome_fantasia",
            "situacao_cadastral", "data_situacao_cadastral",
            "motivo_situacao_cadastral", "nome_cidade_exterior",
            "pais", "data_inicio_atividade",
            "cnae_fiscal_principal", "cnae_fiscal_secundaria",
            "tipo_logradouro", "logradouro", "numero",
            "complemento", "bairro", "cep",
            "uf", "municipio",
            "ddd1", "telefone1",
            "ddd2", "telefone2",
            "ddd_fax", "fax",
            "email",
            "situacao_especial", "data_situacao_especial"
        ]

        tasks = [
            (file, Estabelecimento, columns,
             ["cnpj_basico", "cnpj_ordem", "cnpj_dv"])
            for file in files
        ]

        self._run_parallel(tasks)

    # =====================================================
    # SOCIO
    # =====================================================
    def _process_socio(self):
        files = list(Path(Settings.INPUT_DIR).glob("*.SOCIOCSV"))

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
            (file, Socio, columns, ["cnpj_basico"])
            for file in files
        ]

        self._run_parallel(tasks)

    # =====================================================
    # PIPELINE CORE
    # =====================================================
    def _execute_pipeline(self, file_path, model, columns, key):

        self.logger.info(
            f"Processando {model.__tablename__.upper()}: {file_path.name}"
        )

        db = SessionLocal()
        repo = BulkRepository(db)

        total = 0
        start_time = time.time()

        try:
            for chunk in pd.read_csv(
                file_path,
                sep=";",
                names=columns,
                dtype=str,
                chunksize=self.chunk_size,
                encoding="latin1"
            ):

                chunk_start = time.time()

                # TRANSFORM
                chunk = self.transformer.sanitize(chunk)

                # VALIDATE
                chunk = self._validate(model, chunk)

                # DEDUP
                chunk = self.deduplicator.drop_duplicates(chunk, key)

                # NaN → None
                chunk = chunk.replace({np.nan: None})

                data = chunk.to_dict(orient="records")

                if not data:
                    continue

                # INSERT
                repo.bulk_insert(model, data)

                total += len(data)

                # PERFORMANCE
                elapsed = time.time() - chunk_start
                rps = int(len(data) / elapsed) if elapsed > 0 else 0

                self.logger.info(
                    f"{model.__tablename__} | "
                    f"+{len(data)} | {rps} reg/s | total={total}"
                )

        except Exception as e:
            self.logger.error(
                f"Erro crítico no pipeline {file_path.name}: {e}",
                exc_info=True
            )
            raise

        finally:
            db.close()

        total_time = time.time() - start_time

        self.logger.info(
            f"FINALIZADO {model.__tablename__.upper()} | "
            f"{total} registros | {round(total_time, 2)}s"
        )

    # =====================================================
    # VALIDAÇÃO DINÂMICA
    # =====================================================
    def _validate(self, model, df):

        name = model.__tablename__

        if name == "empresa":
            return self.validator.validate_empresa(df)

        elif name == "estabelecimento":
            return self.validator.validate_estabelecimento(df)

        elif name == "socio":
            return self.validator.validate_socio(df)

        return df