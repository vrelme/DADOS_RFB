# app/etl/raw_import_promotion.py

import logging
import math

import time

from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.database import build_database_url, build_server_url, create_engine_for_database, mysql_connect_args
from app.etl.rfb_manifest import RFB_TABLES, RFB_TABLES_BY_NAME, raw_import_create_table_sql


logger = logging.getLogger(__name__)
error_detail_logger = logging.getLogger("error_detail")


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


class RawImportPromotionRepository:

    def __init__(self):
        self.import_db = Settings.ACTIVE_DB_NAME
        self.final_db = Settings.DB_NAME
        self.import_engine = self._create_promotion_engine(self.import_db)
        self.final_engine = self._create_promotion_engine(self.final_db)
        self.table_timings = {}

    def _log_prefix(self, label: str) -> str:
        return f"{label:<20} |"

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
                f"{self._log_prefix('PROMOCAO')} estrategia=rename_swap | "
                f"{self.import_db} -> {self.final_db}"
            )
            self._swap_all_tables_to_final()
            logger.info(
                f"{self._log_prefix('PROMOCAO')} TEMPO total | {time.time() - started_at:.2f}s"
            )
            return {
                "tables": self.table_timings,
                "total_seconds": time.time() - started_at,
            }

        if not existed:
            logger.info(
                f"{self._log_prefix('PROMOCAO')} Banco final {self.final_db} não existia. Copiando de {self.import_db}."
            )
            self._copy_all_tables_to_final()
            logger.info(
                f"{self._log_prefix('PROMOCAO')} TEMPO total | {time.time() - started_at:.2f}s"
            )
            return {
                "tables": self.table_timings,
                "total_seconds": time.time() - started_at,
            }

        logger.info(
            f"{self._log_prefix('PROMOCAO')} Banco final {self.final_db} já existe. Iniciando comparação com {self.import_db}."
        )
        for table in RFB_TABLES:
            table_started_at = time.time()
            self._compare_or_copy_table(table.table_name, table.columns, table.key_columns)
            if table.table_name not in self.table_timings:
                elapsed = time.time() - table_started_at
                self.table_timings[table.table_name] = elapsed
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} TABELA {table.table_name} | {elapsed:.2f}s"
                )
        logger.info(
            f"{self._log_prefix('PROMOCAO')} TOTAL | {time.time() - started_at:.2f}s"
        )
        return {
            "tables": self.table_timings,
            "total_seconds": time.time() - started_at,
        }

    def _swap_all_tables_to_final(self):
        total_tables = len(RFB_TABLES)
        logger.info(
            f"{self._log_prefix('PROMOCAO')} iniciando troca de {total_tables} tabelas de "
            f"{self.import_db} -> {self.final_db}"
        )
        
        for index, table in enumerate(RFB_TABLES, 1):
            progress_percent = (index / total_tables) * 100
            existed = self._table_exists(self.final_db, table.table_name)
            
            logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) ┌─ INICIO tabela '{table.table_name}'")
            
            if existed:
                # Etapa 1: Auditoria de campos monitorados
                logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  ├─ INICIO auditoria campos monitorados")
                self._audit_monitored_fields(table.table_name)
                logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  └─ FIM auditoria campos monitorados")
            else:
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  ├─ "
                    "Tabela inexistente no banco final; pulando comparacao/auditoria linha-a-linha"
                )
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  └─ "
                    "Promocao sera copia simples por RENAME TABLE"
                )
            
            # Etapa 2: Swap da tabela (RENAME)
            logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  ├─ INICIO swap/rename tabela")
            self._swap_table_to_final(table.table_name)
            logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  └─ FIM swap/rename tabela")
            
            # Etapa 3: Inserção de controle
            logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  ├─ INICIO registro de controle")
            with self.final_engine.begin() as conn:
                status = "promovida" if existed else "copiada"
                detail = (
                    f"Tabela movida por rename_swap de {self.import_db}.{table.table_name} "
                    f"para {self.final_db}.{table.table_name}. "
                    "Sem copia linha-a-linha para reduzir tempo de promocao."
                )
                if not existed:
                    detail += " Tabela final inexistente; comparacao/auditoria foi ignorada por nao haver base anterior."
                self._insert_control(conn, table.table_name, status, detail)
            logger.info(f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) │  └─ FIM registro de controle")
            
            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] ({index}/{total_tables}) └─ FIM tabela '{table.table_name}' {status}"
            )

    def _swap_table_to_final(self, table_name):
        started_at = time.time()
        final_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name)}"
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table_name)}"
        backup_table = f"{quote_identifier(self.final_db)}.{quote_identifier(table_name + '__previous')}"
        final_exists = self._table_exists(self.final_db, table_name)

        logger.info(f"{self._log_prefix('SWAP')} {table_name}: INICIO (final_exists={final_exists})")

        with self.final_engine.begin() as conn:
            # Etapa 1: Criar backup
            logger.info(f"{self._log_prefix('SWAP')} {table_name}: ├─ INICIO criar backup tabela anterior")
            conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
            logger.debug(f"{self._log_prefix('SWAP')} {table_name}: │  └─ DROP IF EXISTS executado")
            
            if final_exists:
                # Etapa 2: RENAME duplo (com backup)
                logger.info(f"{self._log_prefix('SWAP')} {table_name}: ├─ INICIO RENAME duplo (backup + move)")
                logger.debug(f"{self._log_prefix('SWAP')} {table_name}: │  ├─ {final_table} → {backup_table}")
                logger.debug(f"{self._log_prefix('SWAP')} {table_name}: │  └─ {import_table} → {final_table}")
                conn.execute(
                    text(
                        f"RENAME TABLE {final_table} TO {backup_table}, "
                        f"{import_table} TO {final_table}"
                    )
                )
                logger.info(f"{self._log_prefix('SWAP')} {table_name}: │  └─ FIM RENAME duplo")
                
                # Etapa 3: Descartar backup
                logger.info(f"{self._log_prefix('SWAP')} {table_name}: ├─ INICIO descartar backup")
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_table}"))
                logger.info(f"{self._log_prefix('SWAP')} {table_name}: │  └─ FIM descartar backup")
            else:
                # Etapa 2: RENAME simples (primeira vez)
                logger.info(f"{self._log_prefix('SWAP')} {table_name}: ├─ INICIO RENAME simples (primeira vez)")
                logger.debug(f"{self._log_prefix('SWAP')} {table_name}: │  └─ {import_table} → {final_table}")
                conn.execute(text(f"RENAME TABLE {import_table} TO {final_table}"))
                logger.info(f"{self._log_prefix('SWAP')} {table_name}: │  └─ FIM RENAME simples")

        # Etapa 4: Recriar tabela vazia no import
        logger.info(f"{self._log_prefix('SWAP')} {table_name}: ├─ INICIO recriar tabela vazia em {self.import_db}")
        self._recreate_import_table(table_name)
        logger.info(f"{self._log_prefix('SWAP')} {table_name}: │  └─ FIM recriar tabela vazia")
        
        elapsed = time.time() - started_at
        self.table_timings[table_name] = elapsed
        logger.info(f"{self._log_prefix('SWAP')} {table_name}: └─ FIM (tempo total={elapsed:.2f}s)")

    def _recreate_import_table(self, table_name):
        logger.debug(f"{self._log_prefix('RECREATE')} {table_name}: INICIO recriar schema vazio")
        table = RFB_TABLES_BY_NAME[table_name]
        safe_name = quote_identifier(self.import_db)
        with self.import_engine.begin() as conn:
            conn.execute(text(f"USE {safe_name}"))
            logger.debug(f"{self._log_prefix('RECREATE')} {table_name}: executando CREATE TABLE")
            conn.execute(text(raw_import_create_table_sql(table)))
        logger.debug(f"{self._log_prefix('RECREATE')} {table_name}: FIM recriar schema vazio")

    def _audit_monitored_fields(self, table_name):
        if not Settings.MONITORED_FIELD_AUDIT_ENABLED:
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} {table_name}: "
                "auditoria de campos monitorados desabilitada por configuracao"
            )
            return

        fields = self._monitored_fields_for_table(table_name)
        if not fields:
            logger.info(f"{self._log_prefix('MONITORAMENTO')} {table_name}: nenhum campo monitorado")
            return

        started_at = time.time()
        total_fields = len(fields)
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}: INICIO auditoria de {total_fields} campo(s)"
        )
        
        for field_index, field_name in enumerate(fields, 1):
            progress_percent = (field_index / total_fields) * 100
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name} [{int(progress_percent):3d}%] INICIO"
            )
            inserted = self._monitor_field_in_batches(table_name, field_name)
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name} [{int(progress_percent):3d}%] FIM | "
                f"{inserted} alteracoes registradas"
            )
        
        elapsed = time.time() - started_at
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}: FIM auditoria | tempo total={elapsed:.2f}s"
        )

    def _monitored_fields_for_table(self, table_name):
        logger.debug(f"{self._log_prefix('MONITORAMENTO')} {table_name}: INICIO identificar campos monitorados")
        table = RFB_TABLES_BY_NAME[table_name]
        valid_columns = set(table.columns)
        sql = text(
            f"SELECT campo FROM {quote_identifier(self.final_db)}.controle_campo_monitorado "
            "WHERE tabela = :table_name AND ativo = 1"
        )
        with self.final_engine.connect() as conn:
            rows = conn.execute(sql, {"table_name": table_name}).fetchall()

        fields = []
        ignored_count = 0
        for row in rows:
            field_name = row.campo
            if field_name in valid_columns:
                fields.append(field_name)
                logger.debug(f"{self._log_prefix('MONITORAMENTO')} {table_name}: campo '{field_name}' será auditado")
            else:
                ignored_count += 1
                logger.warning(
                    f"{self._log_prefix('MONITORAMENTO')} campo ignorado: {table_name}.{field_name} "
                    "nao existe no manifesto RFB."
                )
        
        logger.debug(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}: FIM identificar campos | "
            f"{len(fields)} validos, {ignored_count} ignorados"
        )
        return fields

    def _monitor_field_in_batches(self, table_name, field_name):
        batch_size = Settings.MONITORED_FIELD_BATCH_SIZE

        if batch_size <= 0:
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: INICIO processamento sem lotes"
            )
            inserted = self._insert_monitored_field_history(table_name, field_name)
            self._refresh_monitored_field_state(table_name, field_name)
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: FIM processamento | {inserted} alteracoes"
            )
            return inserted

        inserted = 0
        offset = 0
        table = RFB_TABLES_BY_NAME[table_name]
        total_rows = self._count_table_rows(table)
        total_batches = math.ceil(total_rows / batch_size) if total_rows > 0 else 0
        started_at = time.time()
        
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: INICIO processamento em lotes"
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: ├─ Total registros: {total_rows}"
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: ├─ Tamanho lote: {batch_size}"
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: ├─ Total lotes: {total_batches}"
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: └─ INICIO comparacao valores (antigos vs novos)"
        )

        batch_count = 0
        while batch_count < total_batches:
            batch_count += 1
            batch_inserted = self._insert_monitored_field_history(
                table_name,
                field_name,
                batch_size=batch_size,
                offset=offset,
            )
            inserted += batch_inserted

            self._refresh_monitored_field_state(
                table_name,
                field_name,
                batch_size=batch_size,
                offset=offset,
            )
            offset += batch_size
            progress_percent = min((offset / total_rows) * 100, 100) if total_rows > 0 else 0
            elapsed = time.time() - started_at
            eta = (
                self._format_duration((elapsed / batch_count) * (total_batches - batch_count))
                if batch_count and total_batches > batch_count
                else "0s"
            )
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: "
                f"[{int(progress_percent):3d}%] lote {batch_count}/{total_batches} | "
                f"{offset}/{total_rows} registros | {batch_inserted} mudancas detectadas | ETA: {eta}"
            )

        elapsed = time.time() - started_at
        throughput = (total_rows / elapsed) if elapsed > 0 else 0
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: └─ FIM comparacao valores"
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}.{field_name}: FIM processamento | "
            f"total={inserted} alteracoes em {batch_count} lotes | "
            f"tempo={self._format_duration(elapsed)} | "
            f"velocidade={throughput:.2f} reg/s"
        )
        return inserted
    
    def _count_table_rows(self, table):
        """Conta o número de registros na tabela de import."""
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table.table_name)}"
        sql = f"SELECT COUNT(*) as cnt FROM {import_table}"
        try:
            with self.final_engine.connect() as conn:
                result = conn.execute(text(sql)).first()
                return result.cnt if result else 0
        except Exception:
            return 0

    def _format_duration(self, seconds):
        seconds = int(seconds or 0)
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h{minutes:02d}m{seconds:02d}s"
        if minutes:
            return f"{minutes}m{seconds:02d}s"
        return f"{seconds}s"

    def _import_batch_has_rows(self, table, batch_size, offset):
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table.table_name)}"
        order_by = ", ".join(quote_identifier(column) for column in table.key_columns)
        sql = (
            f"SELECT 1 FROM {import_table} "
            f"ORDER BY {order_by} "
            f"LIMIT 1 OFFSET {int(offset)}"
        )
        with self.final_engine.connect() as conn:
            return conn.execute(text(sql)).first() is not None

    def _monitoring_source_sql(self, table, alias, batch_size=None, offset=0):
        import_table = f"{quote_identifier(self.import_db)}.{quote_identifier(table.table_name)}"

        if not batch_size or batch_size <= 0:
            return f"{import_table} {alias}"

        order_by = ", ".join(quote_identifier(column) for column in table.key_columns)
        return (
            f"(SELECT * FROM {import_table} "
            f"ORDER BY {order_by} "
            f"LIMIT {int(batch_size)} OFFSET {int(offset)}) {alias}"
        )

    def _monitor_key_expr(self, alias, key_columns):
        parts = [
            f"COALESCE(CAST({alias}.{quote_identifier(column)} AS CHAR), '')"
            for column in key_columns
        ]
        return "CONCAT_WS('|', " + ", ".join(parts) + ")"

    def _insert_monitored_field_history(
        self,
        table_name,
        field_name,
        batch_size=None,
        offset=0,
    ):
        table = RFB_TABLES_BY_NAME[table_name]
        source_sql = self._monitoring_source_sql(table, "n", batch_size, offset)
        state_table = f"{quote_identifier(self.final_db)}.estado_campo_monitorado"
        history_table = f"{quote_identifier(self.final_db)}.historico_campo_monitorado"
        key_expr = self._monitor_key_expr("n", table.key_columns)
        value_expr = f"COALESCE(CAST(n.{quote_identifier(field_name)} AS CHAR), '')"
        now = datetime.now()
        
        logger.debug(
            f"{self._log_prefix('COMPARACAO')} {table_name}.{field_name}: INICIO comparar valores (antigos vs novos)"
        )
        
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
            FROM {source_sql}
            INNER JOIN {state_table} s
                ON s.tabela = :table_name
                AND s.campo = :field_name
                AND s.chave_hash = SHA2({key_expr}, 256)
            WHERE NOT (COALESCE(s.valor_atual, '') = {value_expr})
        """
        result = self._execute_monitoring_sql_with_retries(
            operation="historico",
            table_name=table_name,
            field_name=field_name,
            sql=sql,
            params={
                "table_name": table_name,
                "field_name": field_name,
                "data_movimento": now.date(),
                "hora_movimento": now.time().replace(microsecond=0),
            },
        )
        
        inserted = result.rowcount or 0
        logger.debug(
            f"{self._log_prefix('COMPARACAO')} {table_name}.{field_name}: FIM comparacao | {inserted} mudancas detectadas"
        )
        return inserted

    def _refresh_monitored_field_state(
        self,
        table_name,
        field_name,
        batch_size=None,
        offset=0,
    ):
        logger.debug(
            f"{self._log_prefix('ESTADO')} {table_name}.{field_name}: INICIO atualizar estado de campos"
        )
        table = RFB_TABLES_BY_NAME[table_name]
        source_sql = self._monitoring_source_sql(table, "n", batch_size, offset)
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
            FROM {source_sql}
            ON DUPLICATE KEY UPDATE
                chave = VALUES(chave),
                valor_atual = VALUES(valor_atual),
                updated_at = IF(
                    COALESCE(valor_atual, '') = COALESCE(VALUES(valor_atual), ''),
                    updated_at,
                    VALUES(updated_at)
                )
        """
        self._execute_monitoring_sql_with_retries(
            operation="estado",
            table_name=table_name,
            field_name=field_name,
            sql=sql,
            params={
                "table_name": table_name,
                "field_name": field_name,
            },
        )
        logger.debug(
            f"{self._log_prefix('ESTADO')} {table_name}.{field_name}: FIM atualizar estado de campos"
        )

    def _execute_monitoring_sql_with_retries(
        self,
        operation,
        table_name,
        field_name,
        sql,
        params,
    ):
        last_error = None

        max_retries = max(1, Settings.MAX_RETRIES)

        for attempt in range(1, max_retries + 1):
            try:
                with self.final_engine.begin() as conn:
                    self._prepare_promotion_session(conn)
                    return conn.execute(text(sql), params)

            except SQLAlchemyError as exc:
                last_error = exc

                if self._mysql_error_code(exc) != 1205 or attempt == max_retries:
                    raise

                logger.warning(
                    f"{self._log_prefix('MONITORAMENTO')} lock timeout | "
                    f"{table_name}.{field_name} | operacao={operation} | "
                    f"tentativa={attempt}/{max_retries} | "
                )
                error_detail_logger.warning(
                    f"{self._log_prefix('MONITORAMENTO')} lock timeout | "
                    f"{table_name}.{field_name} | operacao={operation} | "
                    f"tentativa={attempt}/{max_retries} | erro={exc}"
                )
                self._log_database_processes()
                time.sleep(Settings.RETRY_DELAY)

        raise last_error

    def _mysql_error_code(self, error):
        original = getattr(error, "orig", None)
        args = getattr(original, "args", None)

        if args:
            return args[0]

        return None

    def _log_database_processes(self):
        try:
            with self.final_engine.connect() as conn:
                result = conn.execute(text("SHOW FULL PROCESSLIST"))

                for row in result.mappings():
                    info = row.get("Info")
                    if not info:
                        continue

                    logger.warning(
                        f"{self._log_prefix('PROCESSLIST')} "
                        f"Id={row.get('Id')} | "
                        f"User={row.get('User')} | "
                        f"Host={row.get('Host')} | "
                        f"Db={row.get('db') or row.get('Db')} | "
                        f"Command={row.get('Command')} | "
                        f"Time={row.get('Time')} | "
                        f"State={row.get('State')} | "
                        f"Info={str(info)[:500]}"
                    )

        except SQLAlchemyError as exc:
            logger.warning(
                f"{self._log_prefix('PROCESSLIST')} Nao foi possivel consultar SHOW FULL PROCESSLIST: {exc}"
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
                f"{self._log_prefix('MONITORAMENTO')} campo padrao nao foi criado. "
                "Se a tabela controle_campo_monitorado ja e administrada por DBA, "
                f"isso pode ser esperado. Erro: {exc}"
            )

    def _insert_control(self, conn, table_name, status, alteration=None):
        logger.debug(f"{self._log_prefix('CONTROLE')} {table_name}: registrando {status}")
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
        logger.debug(f"{self._log_prefix('CONTROLE')} {table_name}: registro inserido com sucesso")

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
            logger.info(
                f"{self._log_prefix('PROMOCAO')} {table_name}: tabela inexistente em "
                f"{self.final_db}; copiando de {self.import_db} sem comparacao linha-a-linha"
            )
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
        logger.info(f"{self._log_prefix('PROMOCAO')} TABELA {table_name} tempo={elapsed:.2f}s")

    def _copy_table_data(self, table_name, final_table, import_table):
        batch_size = Settings.DB_PROMOTION_BATCH_SIZE
        logger.info(
            f"{self._log_prefix('PROMOCAO')} copiando {self.import_db}.{table_name} -> "
            f"{self.final_db}.{table_name} | "
            f"timeout={Settings.DB_PROMOTION_READ_TIMEOUT}s | lote={batch_size}"
        )
        if batch_size <= 0:
            with self.final_engine.begin() as conn:
                self._prepare_promotion_session(conn)
                conn.execute(
                    text(f"INSERT INTO {final_table} SELECT * FROM {import_table}")
                )
            logger.info(f"{self._log_prefix('PROMOCAO')} {self.import_db}.{table_name} -> {self.final_db}.{table_name}")
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
            logger.info(f"{self._log_prefix('PROMOCAO')} {table_name}: {copied} registros copiados")
            if affected < batch_size:
                break
            offset += batch_size
        logger.info(f"{self._log_prefix('PROMOCAO')} {self.import_db}.{table_name} -> {self.final_db}.{table_name}")

    def _prepare_promotion_session(self, conn):
        timeout = max(Settings.DB_PROMOTION_READ_TIMEOUT, 3600)
        conn.execute(text(f"SET SESSION wait_timeout = {int(timeout)}"))
        conn.execute(text(f"SET SESSION net_read_timeout = {int(timeout)}"))
        conn.execute(text(f"SET SESSION net_write_timeout = {int(timeout)}"))
        conn.execute(
            text(
                "SET SESSION innodb_lock_wait_timeout = "
                f"{int(Settings.DB_PROMOTION_LOCK_WAIT_TIMEOUT)}"
            )
        )
        conn.execute(
            text(
                "SET SESSION lock_wait_timeout = "
                f"{int(Settings.DB_PROMOTION_LOCK_WAIT_TIMEOUT)}"
            )
        )

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
