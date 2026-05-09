# app/etl/bulk_repository.py

import time
import logging
from pathlib import Path

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
    # MYSQL LOAD DATA
    # =====================================================
    def load_file_to_staging(
        self,
        table_name: str,
        file_path: Path
    ):

        loaders = {
            "empresa": self.load_empresa_file_to_staging,
            "estabelecimento": self.load_estabelecimento_file_to_staging,
            "socio": self.load_socio_file_to_staging,
        }

        if table_name not in loaders:
            raise ValueError(
                f"LOAD DATA não configurado para {table_name}"
            )

        return loaders[table_name](file_path)

    def load_empresa_file_to_staging(self, file_path: Path):

        columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo",
        ]

        set_clause = """
            cnpj_basico = IF(
                CHAR_LENGTH(TRIM(@cnpj_basico)) = 8,
                TRIM(@cnpj_basico),
                NULL
            ),
            razao_social = NULLIF(TRIM(@razao_social), ''),
            natureza_juridica =
                CAST(NULLIF(TRIM(@natureza_juridica), '') AS UNSIGNED),
            qualificacao_responsavel =
                CAST(NULLIF(TRIM(@qualificacao_responsavel), '') AS UNSIGNED),
            capital_social =
                CAST(
                    REPLACE(
                        REPLACE(NULLIF(TRIM(@capital_social), ''), '.', ''),
                        ',',
                        '.'
                    )
                    AS DECIMAL(18, 2)
                ),
            porte_empresa =
                CAST(NULLIF(TRIM(@porte_empresa), '') AS UNSIGNED),
            ente_federativo = NULLIF(TRIM(@ente_federativo), '')
        """

        return self._load_data_local_infile(
            table_name="empresa_staging",
            file_path=file_path,
            columns=columns,
            set_clause=set_clause
        )

    def load_estabelecimento_file_to_staging(self, file_path: Path):

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
            "data_situacao_especial",
        ]

        set_clause = self._set_clause_for_text_columns(
            columns,
            required_lengths={
                "cnpj_basico": 8,
                "cnpj_ordem": 4,
                "cnpj_dv": 2,
            }
        )

        return self._load_data_local_infile(
            table_name="estabelecimento_staging",
            file_path=file_path,
            columns=columns,
            set_clause=set_clause
        )

    def load_socio_file_to_staging(self, file_path: Path):

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
            "faixa_etaria",
        ]

        set_clause = self._set_clause_for_text_columns(
            columns,
            required_lengths={
                "cnpj_basico": 8,
            }
        )

        return self._load_data_local_infile(
            table_name="socio_staging",
            file_path=file_path,
            columns=columns,
            set_clause=set_clause
        )

    def _load_data_local_infile(
        self,
        table_name: str,
        file_path: Path,
        columns: list[str],
        set_clause: str
    ):

        start_time = time.time()
        infile = self._mysql_string_literal(file_path)
        user_vars = ", ".join(f"@{column}" for column in columns)

        sql = f"""
            LOAD DATA LOCAL INFILE {infile}
            INTO TABLE {table_name}
            CHARACTER SET {Settings.FILE_ENCODING}
            FIELDS TERMINATED BY '{Settings.FILE_SEPARATOR}'
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({user_vars})
            SET {set_clause}
        """

        try:
            self._prepare_bulk_session()
            result = self.db.execute(text(sql))
            self.db.commit()

            elapsed = round(time.time() - start_time, 2)
            rows = result.rowcount or 0
            rps = int(rows / elapsed) if elapsed > 0 else 0

            logger.info(
                f"{table_name} | LOAD DATA | "
                f"{file_path.name} | {rows} registros | "
                f"{elapsed}s | {rps} reg/s"
            )

            return rows

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                f"Erro LOAD DATA {table_name} "
                f"({file_path.name}): {e}",
                exc_info=True
            )
            raise

    def _prepare_bulk_session(self):

        self.db.execute(
            text("SET SESSION innodb_lock_wait_timeout = 300")
        )
        self.db.execute(text("SET SESSION unique_checks = 0"))
        self.db.execute(text("SET SESSION foreign_key_checks = 0"))

    def _set_clause_for_text_columns(
        self,
        columns: list[str],
        required_lengths: dict[str, int] | None = None
    ):

        required_lengths = required_lengths or {}
        assignments = []

        for column in columns:
            source = f"TRIM(@{column})"

            if column in required_lengths:
                length = required_lengths[column]
                assignments.append(
                    f"""
                    {column} = IF(
                        CHAR_LENGTH({source}) = {length},
                        {source},
                        NULL
                    )
                    """
                )
            else:
                assignments.append(
                    f"{column} = NULLIF({source}, '')"
                )

        return ",\n".join(assignments)

    def _mysql_string_literal(self, file_path: Path):

        value = str(file_path.resolve())
        value = value.replace("\\", "\\\\")
        value = value.replace("'", "''")

        return f"'{value}'"

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

                WHERE cnpj_basico IS NOT NULL

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

                WHERE
                    cnpj_basico IS NOT NULL
                    AND cnpj_ordem IS NOT NULL
                    AND cnpj_dv IS NOT NULL

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

                WHERE cnpj_basico IS NOT NULL
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

        for attempt in range(1, Settings.MAX_RETRIES + 1):

            try:

                # =============================================
                # LOCK TIMEOUT
                # =============================================
                self.db.execute(
                    text(
                        "SET SESSION lock_wait_timeout = 300"
                    )
                )

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

                return

            except SQLAlchemyError as e:

                self.db.rollback()

                logger.warning(
                    f"Erro truncate {table_name} "
                    f"tentativa {attempt}/"
                    f"{Settings.MAX_RETRIES}: {e}"
                )

                self._log_database_processes()

                if attempt == Settings.MAX_RETRIES:
                    logger.error(
                        f"Erro truncate {table_name}: {e}",
                        exc_info=True
                    )
                    raise

                time.sleep(Settings.RETRY_DELAY)

    def _log_database_processes(self):

        try:
            result = self.db.execute(text("SHOW FULL PROCESSLIST"))

            for row in result.mappings():
                info = row.get("Info")
                if not info:
                    continue

                logger.warning(
                    "PROCESSLIST | "
                    f"Id={row.get('Id')} | "
                    f"User={row.get('User')} | "
                    f"Host={row.get('Host')} | "
                    f"Db={row.get('db') or row.get('Db')} | "
                    f"Command={row.get('Command')} | "
                    f"Time={row.get('Time')} | "
                    f"State={row.get('State')} | "
                    f"Info={str(info)[:500]}"
                )

        except SQLAlchemyError as e:
            logger.warning(
                f"Não foi possível consultar SHOW FULL PROCESSLIST: {e}"
            )

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
