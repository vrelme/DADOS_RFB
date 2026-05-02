from pathlib import Path
import logging
import time

from app.database import SessionLocal
from app.config import Settings

from app.etl.loader import CSVLoader
from app.etl.transformer import DataTransformer
from app.etl.validator import Validator
from app.etl.bulk_repository import BulkRepository
from app.etl.schema_mapper import SchemaMapper

from app.models import Empresa, Estabelecimento, Socio


class ETLOrchestrator:

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

        self.loader = CSVLoader()
        self.transformer = DataTransformer()
        self.validator = Validator()

    # =====================================================
    # ENTRYPOINT
    # =====================================================
    def run(self):
        start = time.time()

        self.logger.info("Detectando arquivos RFB...")

        files = sorted(Path(Settings.INPUT_DIR).glob("*"))

        if not files:
            self.logger.warning("Nenhum arquivo encontrado")
            return

        self.logger.info(f"{len(files)} arquivos detectados")

        for file_path in files:
            try:
                self._dispatch(file_path)
            except Exception as e:
                self.logger.error(
                    f"Falha no arquivo {file_path.name}: {e}",
                    exc_info=True
                )

        elapsed = time.time() - start
        self.logger.info(f"ETL finalizado em {elapsed:.2f}s")

    # =====================================================
    # ROTEAMENTO
    # =====================================================
    def _dispatch(self, file_path: Path):

        name = file_path.name.upper()

        if "EMPRECSV" in name:
            self._process_empresa(file_path)

        elif "ESTABELE" in name:
            self._process_estabelecimento(file_path)

        elif "SOCIOCSV" in name:
            self._process_socio(file_path)

        else:
            self.logger.info(f"Ignorado: {file_path.name}")

    # =====================================================
    # PROCESSADORES
    # =====================================================
    def _process_empresa(self, file_path):
        self.logger.info(f"[EMPRESA] {file_path.name}")

        columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel"
        ]

        self._execute_pipeline(Empresa, file_path, columns, key=["cnpj_basico"])

    def _process_estabelecimento(self, file_path):
        self.logger.info(f"[ESTABELECIMENTO] {file_path.name}")

        columns = [
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "nome_fantasia",
            "situacao_cadastral",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "uf",
            "municipio"
        ]

        self._execute_pipeline(
            Estabelecimento,
            file_path,
            columns,
            key=["cnpj_basico", "cnpj_ordem", "cnpj_dv"]
        )

    def _process_socio(self, file_path):
        self.logger.info(f"[SOCIO] {file_path.name}")

        columns = [
            "cnpj_basico",
            "identificador_socio",
            "nome_socio",
            "cpf_cnpj_socio",
            "qualificacao_socio",
            "data_entrada_sociedade"
        ]

        self._execute_pipeline(Socio, file_path, columns, key=None)

    # =====================================================
    # PIPELINE CENTRAL
    # =====================================================
    def _execute_pipeline(self, model, file_path, columns, key=None):

        db = SessionLocal()
        repo = BulkRepository(db)
        mapper = SchemaMapper(model)

        total = 0
        start = time.time()

        try:
            for chunk in self.loader.load(file_path, columns):

                # ---------------------------
                # LOG COLUNAS (debug útil)
                # ---------------------------
                self.logger.debug(f"Colunas: {list(chunk.columns)}")

                # ---------------------------
                # VALIDATION PIPELINE
                # ---------------------------
                chunk = self.validator.normalize(chunk)

                if model.__tablename__ == "empresa":
                    chunk = self.validator.validate_empresa(chunk)

                elif model.__tablename__ == "estabelecimento":
                    chunk = self.validator.validate_estabelecimento(chunk)

                elif model.__tablename__ == "socio":
                    chunk = self.validator.validate_socio(chunk)

                # ---------------------------
                # TRANSFORMAÇÃO
                # ---------------------------
                chunk = self.transformer.sanitize(chunk)

                # ---------------------------
                # MAPEAMENTO (anti erro de schema)
                # ---------------------------
                chunk = mapper.map_columns(chunk)

                # ---------------------------
                # DEDUPLICAÇÃO (crítica)
                # ---------------------------
                if key:
                    chunk = chunk.drop_duplicates(subset=key)

                # ---------------------------
                # CONVERSÃO
                # ---------------------------
                data = chunk.to_dict(orient="records")

                if not data:
                    continue

                # ---------------------------
                # INSERT
                # ---------------------------
                repo.bulk_insert(model, data)

                total += len(data)

                self.logger.info(
                    f"{file_path.name} → {total} registros"
                )

            elapsed = time.time() - start

            self.logger.info(
                f"FINALIZADO {file_path.name} | {total} registros | {elapsed:.2f}s"
            )

        except Exception as e:
            self.logger.error(
                f"Erro crítico no pipeline {file_path.name}: {e}",
                exc_info=True
            )
            raise

        finally:
            db.close()