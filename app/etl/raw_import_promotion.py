# app/etl/raw_import_promotion.py

import logging

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.database import build_server_url, create_engine_for_database, mysql_connect_args
from app.etl.rfb_manifest import RFB_TABLES


logger = logging.getLogger(__name__)


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


class RawImportPromotionRepository:

    def __init__(self):
        self.import_db = Settings.ACTIVE_DB_NAME
        self.final_db = Settings.DB_NAME
        self.import_engine = create_engine_for_database(self.import_db)
        self.final_engine = create_engine_for_database(self.final_db)

    def promote(self):
        existed = self._database_exists(self.final_db)
        self._ensure_database(self.final_db)
        self._ensure_control_table()

        if not existed:
            logger.info(
                f"Banco final {self.final_db} não existia. Copiando tabelas de {self.import_db}."
            )
            self._copy_all_tables_to_final()
            return

        logger.info(
            f"Banco final {self.final_db} já existe. Iniciando comparação com {self.import_db}."
        )
        for table in RFB_TABLES:
            self._compare_or_copy_table(table.table_name, table.columns, table.key_columns)

    def _database_exists(self, database_name):
        server_engine = create_engine_for_database(None)
        # create_engine_for_database(None) points to active DB; use server URL instead.
        server_engine.dispose()
        from sqlalchemy import create_engine

        engine = create_engine(
            build_server_url(),
            future=True,
            pool_pre_ping=Settings.DB_POOL_PRE_PING,
            connect_args=mysql_connect_args(),
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
        sql = f"""
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
        with self.final_engine.begin() as conn:
            conn.execute(text(sql))

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
        with self.final_engine.begin() as conn:
            for table in RFB_TABLES:
                self._replace_final_table(conn, table.table_name)
                self._insert_control(
                    conn,
                    table.table_name,
                    "copiada",
                    f"Tabela criada/copiadada de {self.import_db}.{table.table_name}",
                )

    def _compare_or_copy_table(self, table_name, columns, key_columns):
        if not self._table_exists(self.final_db, table_name):
            with self.final_engine.begin() as conn:
                self._replace_final_table(conn, table_name)
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
            self._replace_final_table(conn, table_name)

    def _replace_final_table(self, conn, table_name):
        final_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name)}"
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        conn.execute(text(f"DROP TABLE IF EXISTS {final_table}"))
        conn.execute(text(f"CREATE TABLE {final_table} LIKE {import_table}"))
        conn.execute(text(f"INSERT INTO {final_table} SELECT * FROM {import_table}"))
        logger.info(f"PROMOÇÃO | {self.import_db}.{table_name} -> {self.final_db}.{table_name}")

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
