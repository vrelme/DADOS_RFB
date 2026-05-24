# app/etl/bulk_repository.py

import time
import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

from app.config import Settings
from app.exceptions import DatabaseOperationError
from app.etl.rfb_manifest import RFB_TABLES_BY_NAME

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

        if table_name in loaders:
            return loaders[table_name](file_path)

        if table_name in RFB_TABLES_BY_NAME:
            return self.load_generic_rfb_file_to_staging(table_name, file_path)

        raise ValueError(
            f"LOAD DATA não configurado para {table_name}"
        )

    def _target_table_name(self, table_name: str):
        if Settings.LOAD_TARGET == "final":
            return table_name
        return f"{table_name}_staging"

    def load_generic_rfb_file_to_staging(self, table_name: str, file_path: Path):

        definition = RFB_TABLES_BY_NAME[table_name]
        columns = list(definition.columns)
        set_clause = self._set_clause_for_text_columns(columns)

        return self._load_data_local_infile(
            table_name=self._target_table_name(table_name),
            file_path=file_path,
            columns=columns,
            set_clause=set_clause
        )

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
            table_name=self._target_table_name("empresa"),
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
            table_name=self._target_table_name("estabelecimento"),
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
            table_name=self._target_table_name("socio"),
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
            logger.info(
                f"{table_name} | INICIO LEITURA | {file_path.name}"
            )
            self._prepare_bulk_session()
            result = self.db.execute(text(sql))
            self.db.commit()

            elapsed = round(time.time() - start_time, 2)
            rows = result.rowcount or 0
            rps = int(rows / elapsed) if elapsed > 0 else 0

            logger.info(
                f"{table_name} | FIM LEITURA | "
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
            raise self._database_operation_error(
                operation="LOAD DATA",
                table_name=table_name,
                error=e
            ) from e

    def _prepare_bulk_session(self):

        self.db.execute(
            text("SET SESSION innodb_lock_wait_timeout = 300")
        )
        self.db.execute(text("SET SESSION unique_checks = 0"))
        self.db.execute(text("SET SESSION foreign_key_checks = 0"))

    def _execute_sql_with_retries(
        self,
        operation: str,
        table_name: str,
        sql
    ):

        last_error = None

        for attempt in range(1, Settings.MAX_RETRIES + 1):
            try:
                self._prepare_bulk_session()
                result = self.db.execute(sql)
                self.db.commit()
                return result

            except SQLAlchemyError as e:
                self.db.rollback()
                last_error = e

                logger.warning(
                    f"{operation} {table_name} falhou "
                    f"tentativa {attempt}/{Settings.MAX_RETRIES}: {e}"
                )

                self._log_database_processes()
                self._handle_lock_timeout(operation, table_name, e)

                if attempt < Settings.MAX_RETRIES:
                    time.sleep(Settings.RETRY_DELAY)

        raise self._database_operation_error(
            operation=operation,
            table_name=table_name,
            error=last_error
        )

    def _database_operation_error(
        self,
        operation: str,
        table_name: str,
        error
    ):

        code = self._mysql_error_code(error)

        if code == 1205:
            message = (
                "Timeout aguardando lock no banco. "
                "Há outra sessão/transação usando esta tabela ou uma tabela "
                "relacionada. Execute SHOW FULL PROCESSLIST, finalize sessões "
                "bloqueadoras com KILL <id>, feche transações abertas no "
                "Workbench e rode novamente. Para carga completa, prefira "
                "MERGE_STRATEGY=full_refresh."
            )
        elif code == 3948:
            message = (
                "LOAD DATA LOCAL INFILE está desabilitado no cliente ou no "
                "servidor. Habilite local_infile no MySQL/MariaDB e mantenha "
                "DB_LOCAL_INFILE=True no .env."
            )
        else:
            message = (
                "Erro operacional no banco. Verifique conectividade, locks, "
                "permissões e o log completo do MySQL/MariaDB."
            )

        return DatabaseOperationError(
            operation=operation,
            table_name=table_name,
            user_message=message,
            original_error=error
        )

    def _mysql_error_code(self, error):

        original = getattr(error, "orig", None)
        args = getattr(original, "args", None)

        if args:
            return args[0]

        return None

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
            if Settings.MERGE_STRATEGY == "full_refresh":
                self.truncate_table("empresa")
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
                """)
            else:
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

            self._execute_sql_with_retries(
                operation="MERGE",
                table_name="empresa",
                sql=sql
            )

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

            raise self._database_operation_error(
                operation="MERGE",
                table_name="empresa",
                error=e
            ) from e

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
            if Settings.MERGE_STRATEGY == "full_refresh":
                self.truncate_table("estabelecimento")
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
                """)
            else:
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

            self._execute_sql_with_retries(
                operation="MERGE",
                table_name="estabelecimento",
                sql=sql
            )

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

            raise self._database_operation_error(
                operation="MERGE",
                table_name="estabelecimento",
                error=e
            ) from e

    # =====================================================
    # MERGE SOCIO
    # =====================================================
    def merge_socio(self):

        logger.info(
            "MERGE socio_staging -> socio"
        )

        start_time = time.time()

        try:
            if Settings.MERGE_STRATEGY == "full_refresh":
                self.truncate_table("socio")

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

            self._execute_sql_with_retries(
                operation="MERGE",
                table_name="socio",
                sql=sql
            )

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

            raise self._database_operation_error(
                operation="MERGE",
                table_name="socio",
                error=e
            ) from e

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
                self._handle_lock_timeout("TRUNCATE", table_name, e)

                if attempt == Settings.MAX_RETRIES:
                    logger.error(
                        f"Erro truncate {table_name}: {e}",
                        exc_info=True
                    )
                    raise self._database_operation_error(
                        operation="TRUNCATE",
                        table_name=table_name,
                        error=e
                    ) from e

                time.sleep(Settings.RETRY_DELAY)

    def _handle_lock_timeout(self, operation, table_name, error):

        if self._mysql_error_code(error) != 1205:
            return

        if not Settings.AUTO_KILL_BLOCKING_SESSIONS:
            return

        killed = self._kill_blocking_sessions(table_name)

        if killed:
            logger.warning(
                f"{operation} {table_name}: sessões bloqueadoras finalizadas: {killed}"
            )
            time.sleep(Settings.AUTO_KILL_WAIT_SECONDS)

    def _kill_blocking_sessions(self, table_name: str):

        killed = []

        try:
            current_id = self.db.execute(text("SELECT CONNECTION_ID()")).scalar()
            result = self.db.execute(text("SHOW FULL PROCESSLIST"))

            for row in result.mappings():
                session_id = row.get("Id")
                db_name = row.get("db") or row.get("Db")
                seconds = int(row.get("Time") or 0)
                info = str(row.get("Info") or "")
                command = str(row.get("Command") or "")

                if session_id == current_id:
                    continue

                if db_name != Settings.DB_NAME:
                    continue

                if seconds < Settings.AUTO_KILL_MIN_SECONDS:
                    continue

                if command.lower() in {"sleep", "binlog dump"}:
                    continue

                if table_name not in info and "_staging" not in info:
                    continue

                logger.warning(
                    f"Finalizando sessão bloqueadora MySQL Id={session_id} | "
                    f"Time={seconds}s | Info={info[:300]}"
                )
                self.db.execute(text(f"KILL {int(session_id)}"))
                killed.append(session_id)

            self.db.commit()

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.warning(f"Não foi possível finalizar sessões bloqueadoras: {e}")

        return killed

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
