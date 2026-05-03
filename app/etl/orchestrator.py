import logging
from pathlib import Path
import time

from app.database import SessionLocal
from app.config import Settings

from app.etl.loader import CSVLoader
from app.etl.transformer import DataTransformer
from app.etl.validator import Validator
from app.etl.bulk_repository import BulkRepository

from app.models import Empresa, Estabelecimento, Socio


class ETLOrchestrator:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.loader = CSVLoader()
        self.transformer = DataTransformer()
        self.validator = Validator()

    # =====================================================
    # ENTRYPOINT
    # =====================================================
    def run(self):
        start = time.time()

        self.logger.info("=" * 70)
        self.logger.info("INICIANDO ETL RFB")
        self.logger.info("=" * 70)

        files = sorted(Path(Settings.INPUT_DIR).glob("*"))

        if not files:
            self.logger.warning("Nenhum arquivo encontrado")
            return

        for file_path in files:
            try:
                self._dispatch(file_path)
            except Exception as e:
                self.logger.exception(f"Falha no arquivo {file_path.name}: {e}")

        elapsed = time.time() - start

        self.logger.info("=" * 70)
        self.logger.info(f"ETL FINALIZADO em {elapsed:.2f}s")
        self.logger.info("=" * 70)

    # =====================================================
    # ROUTER
    # =====================================================
    def _dispatch(self, file_path: Path):

        name = file_path.name.upper()

        if "EMPRE" in name:
            self._process_empresa(file_path)

        elif "ESTABELE" in name:
            self._process_estabelecimento(file_path)

        elif "SOCIO" in name:
            self._process_socio(file_path)

        else:
            self.logger.warning(f"Arquivo ignorado: {name}")

    # =====================================================
    # EMPRESA
    # =====================================================
    def _process_empresa(self, file_path):

        columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo"
        ]

        self._execute_pipeline(
            model=Empresa,
            file_path=file_path,
            columns=columns,
            validator_func=self.validator.validate_empresa
        )

    # =====================================================
    # ESTABELECIMENTO
    # =====================================================
    def _process_estabelecimento(self, file_path):

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

        self._execute_pipeline(
            model=Estabelecimento,
            file_path=file_path,
            columns=columns,
            validator_func=self.validator.validate_estabelecimento
        )

    # =====================================================
    # SOCIO
    # =====================================================
    def _process_socio(self, file_path):

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

        self._execute_pipeline(
            model=Socio,
            file_path=file_path,
            columns=columns,
            validator_func=self.validator.validate_socio
        )

    # =====================================================
    # CORE PIPELINE
    # =====================================================
    def _execute_pipeline(
        self,
        model,
        file_path: Path,
        columns: list,
        validator_func
    ):

        self.logger.info(f"Processando {model.__name__.upper()}: {file_path.name}")

        db = SessionLocal()

        total = 0

        try:
            repo = BulkRepository(db)

            for chunk in self.loader.load(file_path, columns):

                # ---------------------------
                # 1. SANITIZE
                # ---------------------------
                chunk = self.transformer.sanitize(chunk)

                # ---------------------------
                # 2. VALIDATE
                # ---------------------------
                chunk = validator_func(chunk)

                if chunk.empty:
                    continue

                # ---------------------------
                # 3. LOG DE NULOS
                # ---------------------------
                nan_count = chunk.isna().sum().sum()
                if nan_count > 0:
                    self.logger.warning(f"{nan_count} valores NaN convertidos para NULL")

                # ---------------------------
                # 4. CONVERTER PARA DICT
                # ---------------------------
                data = chunk.where(chunk.notna(), None).to_dict(orient="records")

                # ---------------------------
                # 5. INSERT
                # ---------------------------
                repo.bulk_insert(model, data)

                total += len(data)

                if total % 100000 == 0:
                    self.logger.info(f"{file_path.name} → {total} registros")

            self.logger.info(f"{file_path.name} → {total} registros")

        except Exception as e:
            self.logger.exception(f"Erro crítico no pipeline {file_path.name}: {e}")
            raise

        finally:
            db.close()