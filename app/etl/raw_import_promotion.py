# app/etl/raw_import_promotion.py

import logging
import time

from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.database import (
    build_database_url,
    build_server_url,
    create_engine_for_database,
    format_duration,
    mysql_connect_args,
)
from app.etl.rfb_manifest import RFB_TABLES, RFB_TABLES_BY_NAME, raw_import_create_table_sql


logger = logging.getLogger(__name__)
error_detail_logger = logging.getLogger("error_detail")


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


class RawImportPromotionRepository:

    def __init__(self, progress_callback=None, progress_start=0, progress_end=100):
        self.import_db = Settings.ACTIVE_DB_NAME
        self.final_db = Settings.DB_NAME
        self.import_engine = self._create_promotion_engine(self.import_db)
        self.final_engine = self._create_promotion_engine(self.final_db)
        self.table_timings = {}
        self.progress_callback = progress_callback
        self.progress_start = float(progress_start)
        self.progress_end = float(progress_end)
        self._last_progress_percent = self.progress_start

    def _report_progress(self, progress_percent=None, phase=None, table_name=None, detail=None):
        if progress_percent is None:
            progress_percent = self._last_progress_percent
        else:
            self._last_progress_percent = progress_percent

        if not self.progress_callback:
            return

        self.progress_callback(
            progress_percent=progress_percent,
            phase=phase,
            table_name=table_name,
            detail=detail,
        )

    def _promotion_progress(self, completed_tables, total_tables, table_step=0):
        total_tables = total_tables or 1
        table_fraction = (completed_tables + table_step) / total_tables
        return self.progress_start + (
            (self.progress_end - self.progress_start) * table_fraction
        )

    def _log_prefix(self, label: str) -> str:
        return f"{label:<20} |"

    def _format_duration(self, seconds):
        return format_duration(seconds)

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
            completed_tables = index - 1
            existed = self._table_exists(self.final_db, table.table_name)
            self._report_progress(
                progress_percent=self._promotion_progress(completed_tables, total_tables),
                phase="PROMOTE_RAW_IMPORT",
                table_name=table.table_name,
                detail=f"Iniciando tabela {index}/{total_tables}",
            )

            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                f"({index}/{total_tables}) INICIO tabela '{table.table_name}'"
            )

            if existed:
                self._report_progress(
                    progress_percent=self._promotion_progress(completed_tables, total_tables, 0.15),
                    phase="PROMOTE_RAW_IMPORT",
                    table_name=table.table_name,
                    detail="Monitorando CNPJs e socios antes do rename",
                )
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                    f"({index}/{total_tables}) INICIO monitoracao de mudancas"
                )
                self._monitor_business_changes(table.table_name)
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                    f"({index}/{total_tables}) FIM monitoracao de mudancas"
                )
            else:
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                    f"({index}/{total_tables}) Tabela inexistente no banco final; "
                    "pulando monitoracao por nao haver base anterior"
                )
                logger.info(
                    f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                    f"({index}/{total_tables}) Promocao sera copia simples por RENAME TABLE"
                )

            self._report_progress(
                progress_percent=self._promotion_progress(completed_tables, total_tables, 0.65),
                phase="PROMOTE_RAW_IMPORT",
                table_name=table.table_name,
                detail="Executando swap/rename da tabela",
            )
            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                f"({index}/{total_tables}) INICIO swap/rename tabela"
            )
            self._swap_table_to_final(table.table_name)
            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                f"({index}/{total_tables}) FIM swap/rename tabela"
            )

            self._report_progress(
                progress_percent=self._promotion_progress(completed_tables, total_tables, 0.9),
                phase="PROMOTE_RAW_IMPORT",
                table_name=table.table_name,
                detail="Registrando controle da promocao",
            )
            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                f"({index}/{total_tables}) INICIO registro de controle"
            )
            with self.final_engine.begin() as conn:
                status = "promovida" if existed else "copiada"
                detail = (
                    f"Tabela movida por rename_swap de {self.import_db}.{table.table_name} "
                    f"para {self.final_db}.{table.table_name}. "
                    "Sem copia linha-a-linha para reduzir tempo de promocao."
                )
                if not existed:
                    detail += " Tabela final inexistente; monitoracao foi ignorada por nao haver base anterior."
                self._insert_control(conn, table.table_name, status, detail)
            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                f"({index}/{total_tables}) FIM registro de controle"
            )

            logger.info(
                f"{self._log_prefix('PROMOCAO')} [{int(progress_percent):3d}%] "
                f"({index}/{total_tables}) FIM tabela '{table.table_name}' {status}"
            )
            self._report_progress(
                progress_percent=self._promotion_progress(index, total_tables),
                phase="PROMOTE_RAW_IMPORT",
                table_name=table.table_name,
                detail=f"Tabela {index}/{total_tables} promovida",
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

    def _monitor_business_changes(self, table_name):
        if table_name == "estabelecimento":
            self._monitor_estabelecimento_changes()
            return

        if table_name == "socio":
            self._monitor_socio_changes()
            return

        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} {table_name}: sem monitoracao especifica"
        )

    def _monitor_estabelecimento_changes(self):
        started_at = time.time()
        self._ensure_monitoring_indexes(
            "estabelecimento",
            "idx_monitor_estab_chave",
            ("cnpj_basico", "cnpj_ordem", "cnpj_dv"),
        )
        import_table = f"{quote_identifier(self.import_db)}.estabelecimento"
        final_table = f"{quote_identifier(self.final_db)}.estabelecimento"
        detail_table = f"{quote_identifier(self.final_db)}.monitoramento_cnpj_mudanca"
        now = datetime.now()
        movement_date = now.date()
        movement_time = now.time().replace(microsecond=0)
        old_status_expr = "LEFT(TRIM(COALESCE(antigo.situacao_cadastral, '')), 2)"
        new_status_expr = "LEFT(TRIM(COALESCE(novo.situacao_cadastral, '')), 2)"

        sql = f"""
            INSERT INTO {detail_table}
                (tipo_evento, cnpj_basico, cnpj_ordem, cnpj_dv,
                 situacao_anterior, situacao_nova, data_movimento, hora_movimento)
            SELECT
                CASE
                    WHEN antigo.cnpj_basico IS NULL THEN 'cnpj_novo'
                    WHEN {old_status_expr} = '02'
                         AND {new_status_expr} <> '02'
                        THEN 'cnpj_inativado'
                    WHEN {old_status_expr} <> '02'
                         AND {new_status_expr} = '02'
                        THEN 'cnpj_ativado'
                END,
                novo.cnpj_basico,
                novo.cnpj_ordem,
                novo.cnpj_dv,
                antigo.situacao_cadastral,
                novo.situacao_cadastral,
                :data_movimento,
                :hora_movimento
            FROM {import_table} novo
            LEFT JOIN {final_table} antigo
                ON antigo.cnpj_basico = novo.cnpj_basico
                AND antigo.cnpj_ordem = novo.cnpj_ordem
                AND antigo.cnpj_dv = novo.cnpj_dv
            WHERE
                antigo.cnpj_basico IS NULL
                OR (
                    {old_status_expr} = '02'
                    AND {new_status_expr} <> '02'
                )
                OR (
                    {old_status_expr} <> '02'
                    AND {new_status_expr} = '02'
                )
        """
        inserted = self._execute_business_monitoring_sql(
            "estabelecimento",
            sql,
            {
                "data_movimento": movement_date,
                "hora_movimento": movement_time,
            },
        )
        counts = self._monitoring_counts(
            "cnpj_novo",
            "cnpj_ativado",
            "cnpj_inativado",
            movement_date=movement_date,
            movement_time=movement_time,
        )
        self._insert_monitoring_summary(
            source_table="estabelecimento",
            cnpjs_novos=counts["cnpj_novo"],
            cnpjs_ativados=counts["cnpj_ativado"],
            cnpjs_inativados=counts["cnpj_inativado"],
            socios_alterados=0,
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} estabelecimento: "
            f"novos={counts['cnpj_novo']} | ativados={counts['cnpj_ativado']} | "
            f"inativados={counts['cnpj_inativado']} | detalhes={inserted} | "
            f"tempo={self._format_duration(time.time() - started_at)}"
        )

    def _monitor_socio_changes(self):
        started_at = time.time()
        self._ensure_monitoring_indexes(
            "socio",
            "idx_monitor_socio_cnpj",
            ("cnpj_basico",),
        )
        import_table = f"{quote_identifier(self.import_db)}.socio"
        final_table = f"{quote_identifier(self.final_db)}.socio"
        detail_table = f"{quote_identifier(self.final_db)}.monitoramento_cnpj_mudanca"
        now = datetime.now()
        movement_date = now.date()
        movement_time = now.time().replace(microsecond=0)

        socios_hash_expr = """
            SHA2(
                GROUP_CONCAT(
                    SHA2(
                        CONCAT_WS('|',
                            COALESCE(identificador_socio, ''),
                            COALESCE(nome_socio, ''),
                            COALESCE(cpf_cnpj_socio, ''),
                            COALESCE(qualificacao_socio, ''),
                            COALESCE(data_entrada_sociedade, ''),
                            COALESCE(pais, ''),
                            COALESCE(representante_legal, ''),
                            COALESCE(nome_representante, ''),
                            COALESCE(qualificacao_representante_legal, ''),
                            COALESCE(faixa_etaria, '')
                        ),
                        256
                    )
                    ORDER BY identificador_socio, cpf_cnpj_socio, nome_socio, data_entrada_sociedade
                    SEPARATOR ''
                ),
                256
            )
        """
        sql = f"""
            INSERT INTO {detail_table}
                (tipo_evento, cnpj_basico, qtd_socios_anterior, qtd_socios_nova,
                 hash_socios_anterior, hash_socios_nova, data_movimento, hora_movimento)
            SELECT
                'socios_alterados',
                novo.cnpj_basico,
                antigo.qtd_socios,
                novo.qtd_socios,
                antigo.hash_socios,
                novo.hash_socios,
                :data_movimento,
                :hora_movimento
            FROM (
                SELECT
                    cnpj_basico,
                    COUNT(*) AS qtd_socios,
                    {socios_hash_expr} AS hash_socios
                FROM {import_table}
                GROUP BY cnpj_basico
            ) novo
            INNER JOIN (
                SELECT
                    cnpj_basico,
                    COUNT(*) AS qtd_socios,
                    {socios_hash_expr} AS hash_socios
                FROM {final_table}
                GROUP BY cnpj_basico
            ) antigo
                ON antigo.cnpj_basico = novo.cnpj_basico
            WHERE
                COALESCE(antigo.qtd_socios, 0) <> COALESCE(novo.qtd_socios, 0)
                OR COALESCE(antigo.hash_socios, '') <> COALESCE(novo.hash_socios, '')
        """
        inserted = self._execute_business_monitoring_sql(
            "socio",
            sql,
            {
                "data_movimento": movement_date,
                "hora_movimento": movement_time,
            },
        )
        self._insert_monitoring_summary(
            source_table="socio",
            cnpjs_novos=0,
            cnpjs_ativados=0,
            cnpjs_inativados=0,
            socios_alterados=inserted,
        )
        logger.info(
            f"{self._log_prefix('MONITORAMENTO')} socio: socios_alterados={inserted} | "
            f"tempo={self._format_duration(time.time() - started_at)}"
        )

    def _execute_business_monitoring_sql(self, operation, sql, params):
        last_error = None
        max_retries = max(1, Settings.MAX_RETRIES)

        for attempt in range(1, max_retries + 1):
            try:
                with self.final_engine.begin() as conn:
                    self._prepare_promotion_session(conn)
                    result = conn.execute(text(sql), params)
                    return result.rowcount or 0
            except SQLAlchemyError as exc:
                last_error = exc

                if self._mysql_error_code(exc) != 1205 or attempt == max_retries:
                    logger.error(
                        f"{self._log_prefix('MONITORAMENTO')} erro banco | "
                        f"operacao={operation} | tentativa={attempt}/{max_retries} | "
                        f"codigo_mysql={self._mysql_error_code(exc)} | "
                        "detalhes gravados em error.log"
                    )
                    error_detail_logger.error(
                        f"{self._log_prefix('MONITORAMENTO')} erro banco | "
                        f"operacao={operation} | tentativa={attempt}/{max_retries} | "
                        f"codigo_mysql={self._mysql_error_code(exc)} | erro={exc}",
                        exc_info=True,
                    )
                    self._log_database_processes()
                    raise

                logger.warning(
                    f"{self._log_prefix('MONITORAMENTO')} lock timeout | "
                    f"operacao={operation} | tentativa={attempt}/{max_retries}"
                )
                error_detail_logger.warning(
                    f"{self._log_prefix('MONITORAMENTO')} lock timeout | "
                    f"operacao={operation} | tentativa={attempt}/{max_retries} | erro={exc}"
                )
                self._log_database_processes()
                time.sleep(Settings.RETRY_DELAY)

        raise last_error

    def _ensure_monitoring_indexes(self, table_name, index_name, columns):
        for database_name in (self.import_db, self.final_db):
            if self._index_exists(database_name, table_name, index_name):
                continue

            table_ref = f"{quote_identifier(database_name)}.{quote_identifier(table_name)}"
            columns_sql = ", ".join(quote_identifier(column) for column in columns)
            logger.info(
                f"{self._log_prefix('MONITORAMENTO')} criando indice {database_name}.{table_name}.{index_name}"
            )
            self._report_progress(
                phase="PROMOTE_RAW_IMPORT",
                table_name=table_name,
                detail=f"Criando indice {database_name}.{table_name}.{index_name}",
            )
            with self.final_engine.begin() as conn:
                self._prepare_promotion_session(conn)
                conn.execute(
                    text(
                        f"ALTER TABLE {table_ref} "
                        f"ADD INDEX {quote_identifier(index_name)} ({columns_sql})"
                    )
                )
            self._report_progress(
                phase="PROMOTE_RAW_IMPORT",
                table_name=table_name,
                detail=f"Indice criado {database_name}.{table_name}.{index_name}",
            )

    def _index_exists(self, database_name, table_name, index_name):
        with self.final_engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS "
                    "WHERE TABLE_SCHEMA = :schema "
                    "AND TABLE_NAME = :table "
                    "AND INDEX_NAME = :index_name "
                    "LIMIT 1"
                ),
                {
                    "schema": database_name,
                    "table": table_name,
                    "index_name": index_name,
                },
            ).first()
        return result is not None

    def _monitoring_counts(self, *event_types, movement_date, movement_time):
        detail_table = f"{quote_identifier(self.final_db)}.monitoramento_cnpj_mudanca"
        placeholders = ", ".join(f":event_{index}" for index, _ in enumerate(event_types))
        params = {
            "data_movimento": movement_date,
            "hora_movimento": movement_time,
        }
        for index, event_type in enumerate(event_types):
            params[f"event_{index}"] = event_type

        with self.final_engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT tipo_evento, COUNT(*) AS total
                    FROM {detail_table}
                    WHERE data_movimento = :data_movimento
                      AND hora_movimento = :hora_movimento
                      AND tipo_evento IN ({placeholders})
                    GROUP BY tipo_evento
                    """
                ),
                params,
            ).fetchall()

        counts = {event_type: 0 for event_type in event_types}
        counts.update({row.tipo_evento: row.total for row in rows})
        return counts

    def _insert_monitoring_summary(
        self,
        source_table,
        cnpjs_novos,
        cnpjs_ativados,
        cnpjs_inativados,
        socios_alterados,
    ):
        now = datetime.now()
        summary_table = f"{quote_identifier(self.final_db)}.resumo_monitoramento_cnpj"
        detail = (
            f"cnpjs_novos={cnpjs_novos}; "
            f"cnpjs_ativados={cnpjs_ativados}; "
            f"cnpjs_inativados={cnpjs_inativados}; "
            f"socios_alterados={socios_alterados}"
        )

        with self.final_engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {summary_table}
                        (tabela_origem, cnpjs_novos, cnpjs_ativados, cnpjs_inativados,
                         socios_alterados, data_movimento, hora_movimento)
                    VALUES
                        (:tabela_origem, :cnpjs_novos, :cnpjs_ativados, :cnpjs_inativados,
                         :socios_alterados, :data_movimento, :hora_movimento)
                    """
                ),
                {
                    "tabela_origem": source_table,
                    "cnpjs_novos": cnpjs_novos,
                    "cnpjs_ativados": cnpjs_ativados,
                    "cnpjs_inativados": cnpjs_inativados,
                    "socios_alterados": socios_alterados,
                    "data_movimento": now.date(),
                    "hora_movimento": now.time().replace(microsecond=0),
                },
            )
            self._insert_control(conn, source_table, "monitorada", detail)

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
        monitoring_detail_sql = f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(self.final_db)}.monitoramento_cnpj_mudanca (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tipo_evento VARCHAR(50) NOT NULL,
                cnpj_basico VARCHAR(255) NOT NULL,
                cnpj_ordem VARCHAR(255) NULL,
                cnpj_dv VARCHAR(255) NULL,
                situacao_anterior VARCHAR(255) NULL,
                situacao_nova VARCHAR(255) NULL,
                qtd_socios_anterior BIGINT NULL,
                qtd_socios_nova BIGINT NULL,
                hash_socios_anterior CHAR(64) NULL,
                hash_socios_nova CHAR(64) NULL,
                data_movimento DATE NOT NULL,
                hora_movimento TIME NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_monitor_cnpj_tipo_data (tipo_evento, data_movimento),
                INDEX idx_monitor_cnpj_chave (cnpj_basico, cnpj_ordem, cnpj_dv),
                INDEX idx_monitor_cnpj_data (data_movimento, hora_movimento)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        monitoring_summary_sql = f"""
            CREATE TABLE IF NOT EXISTS {quote_identifier(self.final_db)}.resumo_monitoramento_cnpj (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tabela_origem VARCHAR(100) NOT NULL,
                cnpjs_novos BIGINT NOT NULL DEFAULT 0,
                cnpjs_ativados BIGINT NOT NULL DEFAULT 0,
                cnpjs_inativados BIGINT NOT NULL DEFAULT 0,
                socios_alterados BIGINT NOT NULL DEFAULT 0,
                data_movimento DATE NOT NULL,
                hora_movimento TIME NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_resumo_monitor_data (data_movimento, hora_movimento),
                INDEX idx_resumo_monitor_tabela (tabela_origem, data_movimento)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        with self.final_engine.begin() as conn:
            conn.execute(text(control_sql))
            conn.execute(text(monitoring_detail_sql))
            conn.execute(text(monitoring_summary_sql))
            self._ensure_monitoring_table_schema(conn)

    def _ensure_monitoring_table_schema(self, conn):
        table_ref = f"{quote_identifier(self.final_db)}.monitoramento_cnpj_mudanca"
        conn.execute(
            text(
                f"""
                ALTER TABLE {table_ref}
                    MODIFY cnpj_basico VARCHAR(255) NOT NULL,
                    MODIFY cnpj_ordem VARCHAR(255) NULL,
                    MODIFY cnpj_dv VARCHAR(255) NULL,
                    MODIFY situacao_anterior VARCHAR(255) NULL,
                    MODIFY situacao_nova VARCHAR(255) NULL
                """
            )
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
        conn.execute(text("SET SESSION group_concat_max_len = 16777216"))

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
