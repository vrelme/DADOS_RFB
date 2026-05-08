# app/etl/bulk_repository.py

import time
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

from app.config import Settings

logger = logging.getLogger(__name__)


class BulkRepository:

    def __init__(self, db: Session):

        self.db = db

    # =====================================================
    # BULK INSERT
    # =====================================================
    def bulk_insert(
        self,
        model,
        data: list
    ):

        if not data:

            logger.warning(
                f"{model.__tablename__}: lote vazio"
            )

            return

        start_time = time.time()

        try:

            # =============================================
            # MYSQL SESSION OPTIMIZATION
            # =============================================
            self.db.execute(
                text(
                    "SET SESSION innodb_lock_wait_timeout = 120"
                )
            )  # <- incluída no código

            stmt = insert(model).values(data)

            # =============================================
            # IGNORA DUPLICADOS
            # =============================================
            stmt = stmt.prefix_with(
                "IGNORE"
            )

            result = self.db.execute(stmt)

            self.db.commit()

            elapsed = round(
                time.time() - start_time,
                2
            )

            rps = (
                int(len(data) / elapsed)
                if elapsed > 0
                else 0
            )

            logger.info(
                f"{model.__tablename__} | "
                f"{len(data)} registros | "
                f"{elapsed}s | "
                f"{rps} reg/s"
            )

            return result

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro bulk insert "
                f"({model.__tablename__}): {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # MERGE EMPRESA
    # =====================================================
    def merge_empresa(self):

        logger.info(
            "MERGE empresa_staging -> empresa"
        )

        start_time = time.time()

        try:

            # =============================================
            # MYSQL SESSION OPTIMIZATION
            # =============================================
            self.db.execute(
                text(
                    "SET SESSION innodb_lock_wait_timeout = 300"
                )
            )  # <- incluída no código

            sql = text("""
                INSERT INTO empresa (

                    cnpj_basico,
                    razao_social,
                    natureza_juridica,
                    qualificacao_responsavel,
                    capital_social,
                    porte_empresa,
                    ente_federativo
                )

                SELECT

                    cnpj_basico,
                    razao_social,
                    natureza_juridica,
                    qualificacao_responsavel,
                    capital_social,
                    porte_empresa,
                    ente_federativo

                FROM empresa_staging

                ON DUPLICATE KEY UPDATE

                    razao_social =
                        VALUES(razao_social),

                    natureza_juridica =
                        VALUES(natureza_juridica),

                    qualificacao_responsavel =
                        VALUES(qualificacao_responsavel),

                    capital_social =
                        VALUES(capital_social),

                    porte_empresa =
                        VALUES(porte_empresa),

                    ente_federativo =
                        VALUES(ente_federativo)
            """)

            self.db.execute(sql)

            self.db.commit()

            elapsed = round(
                time.time() - start_time,
                2
            )

            logger.info(
                f"MERGE empresa finalizado "
                f"em {elapsed}s"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro MERGE empresa: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # MERGE ESTABELECIMENTO
    # =====================================================
    def merge_estabelecimento(self):

        logger.info(
            "MERGE estabelecimento_staging "
            "-> estabelecimento"
        )

        start_time = time.time()

        try:

            # =============================================
            # MYSQL SESSION OPTIMIZATION
            # =============================================
            self.db.execute(
                text(
                    "SET SESSION innodb_lock_wait_timeout = 300"
                )
            )  # <- incluída no código

            sql = text("""
                INSERT INTO estabelecimento (

                    cnpj_basico,
                    cnpj_ordem,
                    cnpj_dv,
                    identificador_matriz_filial,
                    nome_fantasia,
                    situacao_cadastral,
                    data_situacao_cadastral,
                    motivo_situacao_cadastral,
                    nome_cidade_exterior,
                    pais,
                    data_inicio_atividade,
                    cnae_fiscal_principal,
                    cnae_fiscal_secundaria,
                    tipo_logradouro,
                    logradouro,
                    numero,
                    complemento,
                    bairro,
                    cep,
                    uf,
                    municipio,
                    ddd1,
                    telefone1,
                    ddd2,
                    telefone2,
                    ddd_fax,
                    fax,
                    email,
                    situacao_especial,
                    data_situacao_especial
                )

                SELECT

                    cnpj_basico,
                    cnpj_ordem,
                    cnpj_dv,
                    identificador_matriz_filial,
                    nome_fantasia,
                    situacao_cadastral,
                    data_situacao_cadastral,
                    motivo_situacao_cadastral,
                    nome_cidade_exterior,
                    pais,
                    data_inicio_atividade,
                    cnae_fiscal_principal,
                    cnae_fiscal_secundaria,
                    tipo_logradouro,
                    logradouro,
                    numero,
                    complemento,
                    bairro,
                    cep,
                    uf,
                    municipio,
                    ddd1,
                    telefone1,
                    ddd2,
                    telefone2,
                    ddd_fax,
                    fax,
                    email,
                    situacao_especial,
                    data_situacao_especial

                FROM estabelecimento_staging

                ON DUPLICATE KEY UPDATE

                    nome_fantasia =
                        VALUES(nome_fantasia),

                    situacao_cadastral =
                        VALUES(situacao_cadastral),

                    email =
                        VALUES(email)
            """)

            self.db.execute(sql)

            self.db.commit()

            elapsed = round(
                time.time() - start_time,
                2
            )

            logger.info(
                f"MERGE estabelecimento "
                f"finalizado em {elapsed}s"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro MERGE estabelecimento: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # MERGE SOCIO
    # =====================================================
    def merge_socio(self):

        logger.info(
            "MERGE socio_staging -> socio"
        )

        start_time = time.time()

        try:

            # =============================================
            # MYSQL SESSION OPTIMIZATION
            # =============================================
            self.db.execute(
                text(
                    "SET SESSION innodb_lock_wait_timeout = 300"
                )
            )  # <- incluída no código

            sql = text("""
                INSERT INTO socio (

                    cnpj_basico,
                    identificador_socio,
                    nome_socio,
                    cpf_cnpj_socio,
                    qualificacao_socio,
                    data_entrada_sociedade,
                    pais,
                    representante_legal,
                    nome_representante,
                    qualificacao_representante_legal,
                    faixa_etaria
                )

                SELECT

                    cnpj_basico,
                    identificador_socio,
                    nome_socio,
                    cpf_cnpj_socio,
                    qualificacao_socio,
                    data_entrada_sociedade,
                    pais,
                    representante_legal,
                    nome_representante,
                    qualificacao_representante_legal,
                    faixa_etaria

                FROM socio_staging
            """)

            self.db.execute(sql)

            self.db.commit()

            elapsed = round(
                time.time() - start_time,
                2
            )

            logger.info(
                f"MERGE socio finalizado "
                f"em {elapsed}s"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro MERGE socio: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # TRUNCATE TABLE
    # =====================================================
    def truncate_table(
        self,
        table_name: str
    ):

        logger.info(
            f"Limpando tabela: {table_name}"
        )

        start_time = time.time()

        try:

            # =============================================
            # LOCK TIMEOUT
            # =============================================
            self.db.execute(
                text(
                    "SET SESSION lock_wait_timeout = 30"
                )
            )  # <- incluída no código

            self.db.execute(
                text(
                    f"TRUNCATE TABLE {table_name}"
                )
            )

            self.db.commit()

            elapsed = round(
                time.time() - start_time,
                2
            )

            logger.info(
                f"{table_name} truncada "
                f"em {elapsed}s"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro truncate "
                f"{table_name}: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # EXECUTE SQL
    # =====================================================
    def execute_sql(
        self,
        sql: str
    ):

        try:

            self.db.execute(
                text(sql)
            )

            self.db.commit()

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro SQL customizado: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # RETRY OPERATION
    # =====================================================
    def retry_operation(
        self,
        operation,
        *args,
        **kwargs
    ):

        for attempt in range(
            Settings.MAX_RETRIES
        ):

            try:

                return operation(
                    *args,
                    **kwargs
                )

            except Exception as e:

                logger.warning(
                    f"Tentativa "
                    f"{attempt + 1}/"
                    f"{Settings.MAX_RETRIES} "
                    f"falhou: {e}"
                )

                if (
                    attempt + 1
                    == Settings.MAX_RETRIES
                ):

                    logger.error(
                        "Máximo de tentativas "
                        "atingido"
                    )

                    raise