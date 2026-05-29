# app/etl/raw_import_promotion.py

import logging

import time

from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.database import build_database_url, build_server_url, create_engine_for_database, mysql_connect_args
from app.etl.rfb_manifest import RFB_TABLES, RFB_TABLES_BY_NAME, raw_import_create_table_sql


logger = logging.getLogger(__name__)


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


class RawImportPromotionRepository:

    def __init__(self):
        self.import_db = Settings.ACTIVE_DB_NAME
        self.final_db = Settings.DB_NAME
        self.import_engine = self._create_promotion_engine(self.import_db)
        self.final_engine = self._create_promotion_engine(self.final_db)
        self.table_timings = {}

    def _create_promotion_engine(self, database_name):
        connect_args = mysql_connect_args()
        connect_args["read_timeout"] = Settings.DB_PROMOTION_READ_TIMEOUT
        connect_args["write_timeout"] = Settings.DB_PROMOTION_WRITE_TIMEOUT

        return create_engine(
            build_database_url(database_name),
            echo=Settings.ORM_ECHO,
            future=Settings.ORM_FUTURE,
            pool_size=Settings.DB_POOL_SIZE,
            max_overflow=Settings.DB_MAX_OVERFLOW,
            pool_recycle=Settings.DB_POOL_RECYCLE,
            pool_timeout=Settings.DB_POOL_TIMEOUT,
            pool_pre_ping=Settings.DB_POOL_PRE_PING,
            connect_args=connect_args,
        )

    def promote(self):
        started_at = time.time()
        existed = self._database_exists(self.final_db)
        self._ensure_database(self.final_db)
        self._ensure_control_table()

        if Settings.DB_PROMOTION_STRATEGY == "rename_swap":
            logger.info(
                "PROMOCAO | estrategia=rename_swap | "
                f"{self.import_db} -> {self.final_db}"
            )
            self._swap_all_tables_to_final()
            logger.info(
                f"TEMPO PROMOCAO | total | {time.time() - started_at:.2f}s"
            )
            return {
                "tables": self.table_timings,
                "total_seconds": time.time() - started_at,
            }

        if not existed:
            logger.info(
                f"Banco final {self.final_db} não existia. Copiando tabelas de {self.import_db}."
            )
            self._copy_all_tables_to_final()
            logger.info(
                f"TEMPO PROMOCAO | total | {time.time() - started_at:.2f}s"
            )
            return {
                "tables": self.table_timings,
                "total_seconds": time.time() - started_at,
            }

        logger.info(
            f"Banco final {self.final_db} já existe. Iniciando comparação com {self.import_db}."
        )
        for table in RFB_TABLES:
            table_started_at = time.time()
            self._compare_or_copy_table(table.table_name, table.columns, table.key_columns)
            if table.table_name not in self.table_timings:
                elapsed = time.time() - table_started_at
                self.table_timings[table.table_name] = elapsed
                logger.info(
                    f"TEMPO TABELA | promocao {table.table_name} | {elapsed:.2f}s"
                )
        logger.info(
            f"TEMPO PROMOCAO | total | {time.time() - started_at:.2f}s"
        )
        return {
            "tables": self.table_timings,
            "total_seconds": time.time() - started_at,
        }

    def _swap_all_tables_to_final(self):
        for table in RFB_TABLES:
            existed = self._table_exists(self.final_db, table.table_name)
            self._audit_monitored_fields(table.table_name)
            self._swap_table_to_final(table.table_name)
            with self.final_engine.begin() as conn:
                status = "promovida" if existed else "copiada"
                detail = (
                    f"Tabela movida por rename_swap de {self.import_db}.{table.table_name} "
                    f"para {self.final_db}.{table.table_name}. "
                    "Sem copia linha-a-linha para reduzir tempo de promocao."
                )
                self._insert_control(conn, table.table_name, status, detail)

    def _swap_table_to_final(self, table_name):
        started_at = time.time()
        final_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name)}"
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        backup_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name + '__previous')}"
        final_exists = self._table_exists(self.final_db, table_name)

        with self.final_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            if final_exists:
                conn.execute(
                    text(
                        f"RENAME TABLE {final_table} TO {backup_table}, "
                        f"{import_table} TO {final_table}"
                    )
                )
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            else:
                conn.execute(text(f"RENAME TABLE {import_table} TO {final_table}"))

        self._recreate_import_table(table_name)
        elapsed = time.time() - started_at
        self.table_timings[table_name] = elapsed
        logger.info(f"TEMPO TABELA | promocao {table_name} | {elapsed:.2f}s")

    def _recreate_import_table(self, table_name):
        table = RFB_TABLES_BY_NAME[table_name]
        safe_name = quote_identifier(self.import_db)
        with self.import_engine.begin() as conn:
            conn.execute(text(f"USE {safe_name}"))
            conn.execute(text(raw_import_create_table_sql(table)))

    def _audit_monitored_fields(self, table_name):
        fields = self._monitored_fields_for_table(table_name)
        if not fields:
            return

        started_at = time.time()
        for field_name in fields:
            inserted = self._insert_monitored_field_history(table_name, field_name)
            self._refresh_monitored_field_state(table_name, field_name)
            logger.info(
                f"MONITORAMENTO | {table_name}.{field_name} | "
                f"{inserted} alteracoes registradas"
            )
        logger.info(
            f"TEMPO MONITORAMENTO | {table_name} | {time.time() - started_at:.2f}s"
        )

    def _monitored_fields_for_table(self, table_name):
        table = RFB_TABLES_BY_NAME[table_name]
        valid_columns = set(table.columns)
        sql = text(
            f"SELECT campo FROM {quote_identifier(self.final_db)}.controle_campo_monitorado "
            "WHERE tabela = :table_name AND ativo = 1"
        )
        with self.final_engine.connect() as conn:
            rows = conn.execute(sql, {"table_name": table_name}).fetchall()

        fields = []
        for row in rows:
            field_name = row.campo
            if field_name in valid_columns:
                fields.append(field_name)
            else:
                logger.warning(
                    f"MONITORAMENTO | campo ignorado: {table_name}.{field_name} "
                    "nao existe no manifesto RFB."
                )
        return fields

    def _monitor_key_expr(self, alias, key_columns):
        parts = [
            f"COALESCE(CAST({alias}.{quote_identifier(column)} AS CHAR), '')"
            for column in key_columns
        ]
        return "CONCAT_WS('|', " + ", ".join(parts) + ")"

    def _insert_monitored_field_history(self, table_name, field_name):
        table = RFB_TABLES_BY_NAME[table_name]
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        state_table = f"{quote_identifier(self.final_db)}.estado_campo_monitorado"
        history_table = f"{quote_identifier(self.final_db)}.historico_campo_monitorado"
        key_expr = self._monitor_key_expr("n", table.key_columns)
        value_expr = f"COALESCE(CAST(n.{quote_identifier(field_name)} AS CHAR), '')"
        now = datetime.now()
        sql = f"""
            INSERT INTO {history_table}
                (tabela, campo, chave_hash, chave, valor_anterior, valor_novo,
                 data_movimento, hora_movimento)
            SELECT
                :table_name,
                :field_name,
                SHA2({key_expr}, 256),
                {key_expr},
                s.valor_atual,
                {value_expr},
                :data_movimento,
                :hora_movimento
            FROM {import_table} n
            INNER JOIN {state_table} s
                ON s.tabela = :table_name
                AND s.campo = :field_name
                AND s.chave_hash = SHA2({key_expr}, 256)
            WHERE NOT (COALESCE(s.valor_atual, '') = {value_expr})
        """
        with self.final_engine.begin() as conn:
            self._prepare_promotion_session(conn)
            result = conn.execute(
                text(sql),
                {
                    "table_name": table_name,
                    "field_name": field_name,
                    "data_movimento": now.date(),
                    "hora_movimento": now.time().replace(microsecond=0),
                },
            )
            return result.rowcount or 0

    def _refresh_monitored_field_state(self, table_name, field_name):
        table = RFB_TABLES_BY_NAME[table_name]
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        state_table = f"{quote_identifier(self.final_db)}.estado_campo_monitorado"
        key_expr = self._monitor_key_expr("n", table.key_columns)
        value_expr = f"COALESCE(CAST(n.{quote_identifier(field_name)} AS CHAR), '')"
        sql = f"""
            INSERT INTO {state_table}
                (tabela, campo, chave_hash, chave, valor_atual, updated_at)
            SELECT
                :table_name,
                :field_name,
                SHA2({key_expr}, 256),
                {key_expr},
                {value_expr},
                NOW()
            FROM {import_table} n
            ON DUPLICATE KEY UPDATE
                chave = VALUES(chave),
                valor_atual = VALUES(valor_atual),
                updated_at = IF(
                    COALESCE(valor_atual, '') = COALESCE(VALUES(valor_atual), ''),
                    updated_at,
                    VALUES(updated_at)
                )
        """
        with self.final_engine.begin() as conn:
            self._prepare_promotion_session(conn)
            conn.execute(
                text(sql),
                {
                    "table_name": table_name,
                    "field_name": field_name,
                },
            )

    def _database_exists(self, database_name):
        server_engine = create_engine_for_database(None)
        # create_engine_for_database(None) points to active DB; use server URL instead.
        server_engine.dispose()
        connect_args = mysql_connect_args()
        connect_args["read_timeout"] = Settings.DB_PROMOTION_READ_TIMEOUT
        connect_args["write_timeout"] = Settings.DB_PROMOTION_WRITE_TIMEOUT

        engine = create_engine(
            build_server_url(),
            future=True,
            pool_pre_ping=Settings.DB_POOL_PRE_PING,
            connect_args=connect_args,
        )
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :db"),
                    {"db": database_name},
                ).first()
                return result is not None
        finally:
            engine.dispose()

    def _ensure_database(self, database_name):
        from app.database import ensure_database_exists

        ensure_database_exists(database_name)

    def _table_exists(self, database_name, table_name):
        with self.final_engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                ),
                {"schema": database_name, "table": table_name},
            ).first()
            return result is not None

    def _ensure_control_table(self):
        control_sql = f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(self.final_db)}.controle_alteracao (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tabela VARCHAR(100) NOT NULL,
                status VARCHAR(50) NOT NULL,
                alteracao LONGTEXT NULL,
                data_movimento DATE NOT NULL,
                hora_movimento TIME NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_controle_tabela_data (tabela, data_movimento),
                INDEX idx_controle_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        monitored_sql = f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(self.final_db)}.controle_campo_monitorado (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tabela VARCHAR(100) NOT NULL,
                campo VARCHAR(100) NOT NULL,
                ativo TINYINT(1) NOT NULL DEFAULT 1,
                observacao VARCHAR(500) NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_controle_campo_monitorado (tabela, campo),
                INDEX idx_controle_campo_ativo (ativo, tabela)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        state_sql = f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(self.final_db)}.estado_campo_monitorado (
                tabela VARCHAR(100) NOT NULL,
                campo VARCHAR(100) NOT NULL,
                chave_hash CHAR(64) NOT NULL,
                chave LONGTEXT NOT NULL,
                valor_atual LONGTEXT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (tabela, campo, chave_hash),
                INDEX idx_estado_tabela_campo (tabela, campo)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        history_sql = f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(self.final_db)}.historico_campo_monitorado (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tabela VARCHAR(100) NOT NULL,
                campo VARCHAR(100) NOT NULL,
                chave_hash CHAR(64) NOT NULL,
                chave LONGTEXT NOT NULL,
                valor_anterior LONGTEXT NULL,
                valor_novo LONGTEXT NULL,
                data_movimento DATE NOT NULL,
                hora_movimento TIME NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_hist_monitor_tabela_campo_data (tabela, campo, data_movimento),
                INDEX idx_hist_monitor_hash (tabela, campo, chave_hash)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        with self.final_engine.begin() as conn:
            conn.execute(text(control_sql))
            conn.execute(text(monitored_sql))
            conn.execute(text(state_sql))
            conn.execute(text(history_sql))
        self._seed_default_monitored_fields()

    def _seed_default_monitored_fields(self):
        if not Settings.MONITORED_FIELDS_BOOTSTRAP_DEFAULTS:
            return

        try:
            with self.final_engine.begin() as conn:
                conn.execute(
                    text(
                        f"INSERT IGNORE INTO {quote_identifier(self.final_db)}.controle_campo_monitorado "
                        "(tabela, campo, ativo, observacao) "
                        "VALUES ('estabelecimento', 'situacao_cadastral', 1, "
                        "'Campo padrao para historico de mudanca ativa/inativa.')"
                    )
                )
        except SQLAlchemyError as exc:
            logger.warning(
                "MONITORAMENTO | campo padrao nao foi criado. "
                "Se a tabela controle_campo_monitorado ja e administrada por DBA, "
                f"isso pode ser esperado. Erro: {exc}"
            )

    def _insert_control(self, conn, table_name, status, alteration=None):
        now = datetime.now()
        conn.execute(
            text(
                f"INSERT INTO {quote_identifier(self.final_db)}.controle_alteracao "
                "(tabela, status, alteracao, data_movimento, hora_movimento) "
                "VALUES (:tabela, :status, :alteracao, :data_movimento, :hora_movimento)"
            ),
            {
                "tabela": table_name,
                "status": status,
                "alteracao": alteration,
                "data_movimento": now.date(),
                "hora_movimento": now.time().replace(microsecond=0),
            },
        )

    def _copy_all_tables_to_final(self):
        for table in RFB_TABLES:
            self._replace_final_table(table.table_name)
            with self.final_engine.begin() as conn:
                self._insert_control(
                    conn,
                    table.table_name,
                    "copiada",
                    f"Tabela criada/copiadada de {self.import_db}.{table.table_name}",
                )

    def _compare_or_copy_table(self, table_name, columns, key_columns):
        if not self._table_exists(self.final_db, table_name):
            self._replace_final_table(table_name)
            with self.final_engine.begin() as conn:
                self._insert_control(
                    conn,
                    table_name,
                    "copiada",
                    f"Tabela inexistente no banco final. Copiada de {self.import_db}.{table_name}",
                )
            return

        old_count, old_checksum = self._table_signature(self.final_db, table_name, columns)
        new_count, new_checksum = self._table_signature(self.import_db, table_name, columns)

        with self.final_engine.begin() as conn:
            if old_count == new_count and old_checksum == new_checksum:
                self._insert_control(conn, table_name, "sem alteração", None)
                return

            summary = (
                f"Divergência detectada: registros_anterior={old_count}; "
                f"registros_novo={new_count}; checksum_anterior={old_checksum}; "
                f"checksum_novo={new_checksum}"
            )
            self._insert_control(conn, table_name, "tem alteração", summary)
            if table_name in Settings.CONTROL_DIFF_DETAIL_TABLES:
                self._insert_detail_changes(conn, table_name, columns, key_columns)
            else:
                self._insert_control(
                    conn,
                    table_name,
                    "tem alteração",
                    "Detalhe campo-a-campo ignorado por configuração para preservar performance.",
                )
        self._replace_final_table(table_name)

    def _replace_final_table(self, table_name):
        started_at = time.time()
        final_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name)}"
        temp_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name + '__promoting')}"
        backup_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name + '__previous')}"
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        final_exists = self._table_exists(self.final_db, table_name)
        with self.final_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            conn.execute(text(f"CREATE TABLE {temp_table} LIKE {import_table}"))
            self._prepare_promotion_session(conn)
        self._copy_table_data(table_name, temp_table, import_table)
        with self.final_engine.begin() as conn:
            if final_exists:
                conn.execute(
                    text(
                        f"RENAME TABLE {final_table} TO {backup_table}, "
                        f"{temp_table} TO {final_table}"
                    )
                )
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            else:
                conn.execute(text(f"RENAME TABLE {temp_table} TO {final_table}"))
        elapsed = time.time() - started_at
        self.table_timings[table_name] = elapsed
        logger.info(f"TEMPO TABELA | promocao {table_name} | {elapsed:.2f}s")

    def _copy_table_data(self, table_name, final_table, import_table):
        batch_size = Settings.DB_PROMOTION_BATCH_SIZE
        logger.info(
            f"PROMOÇÃO | copiando {self.import_db}.{table_name} -> "
            f"{self.final_db}.{table_name} | "
            f"timeout={Settings.DB_PROMOTION_READ_TIMEOUT}s | lote={batch_size}"
        )
        if batch_size <= 0:
            with self.final_engine.begin() as conn:
                self._prepare_promotion_session(conn)
                conn.execute(
                    text(f"INSERT INTO {final_table} SELECT * FROM {import_table}")
                )
            logger.info(f"PROMOCAO | {self.import_db}.{table_name} -> {self.final_db}.{table_name}")
            return

        offset = 0
        copied = 0
        while True:
            with self.final_engine.begin() as conn:
                self._prepare_promotion_session(conn)
                result = conn.execute(
                    text(
                        f"INSERT INTO {final_table} "
                        f"SELECT * FROM {import_table} "
                        f"LIMIT {int(batch_size)} OFFSET {int(offset)}"
                    )
                )
                affected = result.rowcount or 0
            copied += affected
            if affected == 0:
                break
            logger.info(f"PROMOCAO | {table_name}: {copied} registros copiados")
            if affected < batch_size:
                break
            offset += batch_size
        logger.info(f"PROMOÇÃO | {self.import_db}.{table_name} -> {self.final_db}.{table_name}")

    def _prepare_promotion_session(self, conn):
        timeout = max(Settings.DB_PROMOTION_READ_TIMEOUT, 3600)
        conn.execute(text(f"SET SESSION wait_timeout = {int(timeout)}"))
        conn.execute(text(f"SET SESSION net_read_timeout = {int(timeout)}"))
        conn.execute(text(f"SET SESSION net_write_timeout = {int(timeout)}"))

    def _table_signature(self, database_name, table_name, columns):
        table_ref = f"{quote_identifier(database_name)}.{quote_identifier(table_name)}"
        concat_expr = self._concat_row_expr(columns)
        sql = f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(BIT_XOR(CAST(CRC32({concat_expr}) AS UNSIGNED)), 0) AS checksum
            FROM {table_ref}
        """
        with self.final_engine.connect() as conn:
            row = conn.execute(text(sql)).one()
            return int(row.total or 0), int(row.checksum or 0)

    def _concat_row_expr(self, columns, alias=None):
        prefix = f"{alias}." if alias else ""
        parts = [f"COALESCE(CAST({prefix}{quote_identifier(column)} AS CHAR), '')" for column in columns]
        return "CONCAT_WS('|', " + ", ".join(parts) + ")"

    def _key_condition(self, left_alias, right_alias, key_columns):
        return " AND ".join(
            f"COALESCE({left_alias}.{quote_identifier(column)}, '') = "
            f"COALESCE({right_alias}.{quote_identifier(column)}, '')"
            for column in key_columns
        )

    def _key_text(self, alias, key_columns):
        parts = [
            f"CONCAT('{column}=', COALESCE(CAST({alias}.{quote_identifier(column)} AS CHAR), ''))"
            for column in key_columns
        ]
        return "CONCAT_WS('; ', " + ", ".join(parts) + ")"

    def _insert_detail_changes(self, conn, table_name, columns, key_columns):
        max_rows = Settings.CONTROL_DIFF_MAX_ROWS
        if max_rows <= 0:
            return

        inserted = 0
        inserted += self._insert_missing_or_new_rows(conn, table_name, key_columns, "novo", max_rows - inserted)
        if inserted >= max_rows:
            return
        inserted += self._insert_missing_or_new_rows(conn, table_name, key_columns, "removido", max_rows - inserted)
        if inserted >= max_rows:
            return

        non_key_columns = [column for column in columns if column not in key_columns]
        for column in non_key_columns:
            if inserted >= max_rows:
                break
            inserted += self._insert_column_changes(
                conn,
                table_name,
                key_columns,
                column,
                max_rows - inserted,
            )

    def _insert_missing_or_new_rows(self, conn, table_name, key_columns, mode, limit):
        if limit <= 0:
            return 0

        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        final_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name)}"
        if mode == "novo":
            left_ref, right_ref = import_table, final_table
            status = "tem alteração"
            description = "registro novo"
        else:
            left_ref, right_ref = final_table, import_table
            status = "tem alteração"
            description = "registro removido"

        first_key = key_columns[0]
        sql = f"""
            INSERT INTO {quote_identifier(self.final_db)}.controle_alteracao
                (tabela, status, alteracao, data_movimento, hora_movimento)
            SELECT
                :table_name,
                :status,
                CONCAT(:description, '; chave=', {self._key_text('a', key_columns)}),
                :data_movimento,
                :hora_movimento
            FROM {left_ref} a
            LEFT JOIN {right_ref} b ON {self._key_condition('a', 'b', key_columns)}
            WHERE b.{quote_identifier(first_key)} IS NULL
            LIMIT {int(limit)}
        """
        now = datetime.now()
        result = conn.execute(
            text(sql),
            {
                "table_name": table_name,
                "status": status,
                "description": description,
                "data_movimento": now.date(),
                "hora_movimento": now.time().replace(microsecond=0),
            },
        )
        return result.rowcount or 0

    def _insert_column_changes(self, conn, table_name, key_columns, column, limit):
        if limit <= 0:
            return 0

        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        final_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name)}"
        column_ref = quote_identifier(column)
        sql = f"""
            INSERT INTO {quote_identifier(self.final_db)}.controle_alteracao
                (tabela, status, alteracao, data_movimento, hora_movimento)
            SELECT
                :table_name,
                'tem alteração',
                CONCAT(
                    'campo=', :column_name,
                    '; chave=', {self._key_text('n', key_columns)},
                    '; anterior=', COALESCE(CAST(o.{column_ref} AS CHAR), ''),
                    '; novo=', COALESCE(CAST(n.{column_ref} AS CHAR), '')
                ),
                :data_movimento,
                :hora_movimento
            FROM {import_table} n
            INNER JOIN {final_table} o ON {self._key_condition('n', 'o', key_columns)}
            WHERE NOT (
                COALESCE(CAST(n.{column_ref} AS CHAR), '') =
                COALESCE(CAST(o.{column_ref} AS CHAR), '')
            )
            LIMIT {int(limit)}
        """
        now = datetime.now()
        result = conn.execute(
            text(sql),
            {
                "table_name": table_name,
                "column_name": column,
                "data_movimento": now.date(),
                "hora_movimento": now.time().replace(microsecond=0),
            },
        )
        return result.rowcount or 0
