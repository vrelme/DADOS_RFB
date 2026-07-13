import logging
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import bindparam, create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from app.config import Settings
from app.database import (
    OperationalSessionLocal,
    build_database_url,
    is_database_connection_lost,
    mysql_connect_args,
)
from app.models import ETLExecution, ETLFileProgress, ETLMetric, ETLRun, ETLRunPhase


logger = logging.getLogger(__name__)


class MetricsRepository:
    """Read-only queries used by the metrics API and Prometheus collector."""

    def __init__(self):
        self.ops_session_factory = OperationalSessionLocal
        self.final_engine = self._create_metrics_engine(Settings.DB_NAME)
        self._table_exists_cache: Dict[str, bool] = {}

    def _create_metrics_engine(self, database_name: str):
        timeout = max(1, Settings.METRICS_DB_QUERY_TIMEOUT)
        return create_engine(
            build_database_url(database_name),
            echo=False,
            future=True,
            poolclass=NullPool,
            pool_pre_ping=True,
            connect_args=mysql_connect_args(
                connect_timeout=timeout,
                read_timeout=timeout,
                write_timeout=timeout,
            ),
        )

    def get_overview_metrics(self) -> Dict[str, Any]:
        db = self.ops_session_factory()
        try:
            run = db.query(ETLRun).order_by(ETLRun.started_at.desc()).first()
            if run:
                return {
                    "last_execution": self._iso(run.started_at),
                    "status": run.status,
                    "total_duration_seconds": int(run.duration_seconds or 0),
                    "files_processed": int(
                        (run.completed_files or 0) + (run.failed_files or 0)
                    ),
                    "records_loaded": int(run.total_records or 0),
                    "errors": int(run.failed_files or 0),
                }

            last = (
                db.query(ETLExecution)
                .order_by(ETLExecution.started_at.desc())
                .first()
            )
            if not last:
                return self._empty_overview()

            started_at = last.started_at
            same_run_rows = (
                db.query(ETLExecution)
                .filter(ETLExecution.started_at <= started_at)
                .all()
            )
            return {
                "last_execution": self._iso(started_at),
                "status": last.status,
                "total_duration_seconds": int(last.duration_seconds or 0),
                "files_processed": len(same_run_rows),
                "records_loaded": int(
                    sum(row.records_processed or 0 for row in same_run_rows)
                ),
                "errors": int(
                    sum(1 for row in same_run_rows if row.status == "FAILED")
                ),
            }
        except SQLAlchemyError as exc:
            logger.error("Erro ao consultar overview de metricas: %s", exc, exc_info=True)
            return self._empty_overview()
        finally:
            db.close()

    def get_performance_metrics(self) -> Dict[str, Any]:
        db = self.ops_session_factory()
        try:
            file_rows = (
                db.query(ETLFileProgress)
                .order_by(ETLFileProgress.started_at.desc())
                .limit(100)
                .all()
            )
            stage_rows = (
                db.query(ETLRunPhase)
                .order_by(ETLRunPhase.started_at.desc())
                .limit(100)
                .all()
            )
            metric_rows = (
                db.query(ETLMetric)
                .filter(
                    ETLMetric.metric_name.in_(
                        ["merge_duration_seconds", "merge_seconds", "rename_swap_seconds"]
                    )
                )
                .order_by(ETLMetric.created_at.desc())
                .limit(100)
                .all()
            )

            file_durations = [
                {
                    "pipeline": row.pipeline,
                    "file_name": row.file_name,
                    "table_name": row.table_name,
                    "stage": "file",
                    "status": row.status,
                    "duration_seconds": int(row.duration_seconds or 0),
                    "records": int(row.records_processed or 0),
                    "records_per_second": float(
                        row.throughput_rows_per_second or 0
                    ),
                }
                for row in file_rows
            ]
            stage_durations = [
                {
                    "pipeline": "RFB_LOADER_ENTERPRISE",
                    "file_name": "",
                    "table_name": row.table_name or "",
                    "stage": row.phase_name,
                    "status": row.status,
                    "duration_seconds": int(row.duration_seconds or 0),
                }
                for row in stage_rows
            ]
            merge_duration = self._latest_metric_value(metric_rows, "merge")
            rename_swap_duration = self._latest_metric_value(metric_rows, "rename_swap")
            if not rename_swap_duration:
                rename_swap_duration = self._latest_promotion_duration()

            records = sum(item["records"] for item in file_durations)
            duration = sum(item["duration_seconds"] for item in file_durations)

            return {
                "records_per_second": float(records / duration) if duration else 0,
                "file_durations": file_durations,
                "stage_durations": stage_durations,
                "merge_duration_seconds": float(merge_duration or 0),
                "rename_swap_duration_seconds": float(rename_swap_duration or 0),
            }
        except SQLAlchemyError as exc:
            logger.error("Erro ao consultar performance: %s", exc, exc_info=True)
            return {
                "records_per_second": 0,
                "file_durations": [],
                "stage_durations": [],
                "merge_duration_seconds": 0,
                "rename_swap_duration_seconds": 0,
            }
        finally:
            db.close()

    def get_database_counts(self) -> Dict[str, int]:
        empty_counts = self._empty_database_counts()

        if Settings.METRICS_DATABASE_COUNTS_MODE == "disabled":
            return empty_counts

        if Settings.METRICS_DATABASE_COUNTS_MODE != "exact":
            estimated = self._estimated_database_counts()
            if estimated is not None:
                return estimated
            return empty_counts

        tables = self._database_count_tables()
        return {
            key: self._count_first_existing_table(*table_names)
            for key, table_names in tables.items()
        }

    def get_execution_history(self, limit: int = 100) -> Dict[str, Any]:
        db = self.ops_session_factory()
        try:
            rows = (
                db.query(ETLExecution)
                .order_by(ETLExecution.started_at.desc())
                .limit(limit)
                .all()
            )
            return {
                "executions": [
                    {
                        "pipeline": row.pipeline,
                        "worker": row.worker or "",
                        "file_name": row.file_name,
                        "table_name": row.table_name,
                        "date": self._iso(row.started_at),
                        "status": row.status,
                        "duration_seconds": int(row.duration_seconds or 0),
                        "records": int(row.records_processed or 0),
                    }
                    for row in rows
                ]
            }
        except SQLAlchemyError as exc:
            logger.error("Erro ao consultar historico de execucao: %s", exc, exc_info=True)
            return {"executions": []}
        finally:
            db.close()

    def get_promotion_metrics(self, limit: int = 100) -> Dict[str, Any]:
        if not self._table_exists(Settings.DB_NAME, "controle_alteracao"):
            return {"promotions": []}

        sql = text(
            """
            SELECT tabela, status, alteracao, data_movimento, hora_movimento, created_at
            FROM controle_alteracao
            ORDER BY COALESCE(created_at, data_movimento) DESC, id DESC
            LIMIT :limit
            """
        )
        try:
            with self.final_engine.connect() as conn:
                rows = conn.execute(sql, {"limit": int(limit)}).mappings().all()
            return {
                "promotions": [
                    {
                        "table_name": row.get("tabela") or "",
                        "strategy": self._promotion_strategy(row),
                        "status": row.get("status") or "",
                        "date": self._combine_date_time(
                            row.get("created_at"),
                            row.get("data_movimento"),
                            row.get("hora_movimento"),
                        ),
                        "duration_seconds": 0,
                    }
                    for row in rows
                ]
            }
        except SQLAlchemyError as exc:
            logger.error("Erro ao consultar metricas de promocao: %s", exc, exc_info=True)
            return {"promotions": []}

    def get_audit_metrics(self, limit: int = 100) -> Dict[str, Any]:
        history = self._audit_history_from_monitoring_tables(limit)
        fields_added = sum(1 for item in history if item["event_type"] == "added")
        fields_removed = sum(1 for item in history if item["event_type"] == "removed")
        fields_changed = sum(1 for item in history if item["event_type"] == "changed")
        return {
            "fields_added": fields_added,
            "fields_removed": fields_removed,
            "fields_changed": fields_changed,
            "history": history,
        }

    def _audit_history_from_monitoring_tables(self, limit: int) -> List[Dict[str, Any]]:
        for table_name in ("historico_campo_monitorado", "estado_campo_monitorado"):
            if not self._table_exists(Settings.DB_NAME, table_name):
                continue

            columns = self._table_columns(Settings.DB_NAME, table_name)
            table_col = self._first_existing(columns, "table_name", "tabela")
            field_col = self._first_existing(
                columns,
                "field_name",
                "campo",
                "nome_campo",
                "column_name",
            )
            event_col = self._first_existing(
                columns,
                "event_type",
                "tipo_evento",
                "tipo_alteracao",
                "status",
            )
            date_col = self._first_existing(
                columns,
                "created_at",
                "data_evento",
                "data_movimento",
                "updated_at",
            )
            if not all([table_col, field_col, event_col]):
                continue

            date_select = f"`{date_col}`" if date_col else "NULL"
            sql = text(
                f"""
                SELECT
                    `{table_col}` AS table_name,
                    `{field_col}` AS field_name,
                    `{event_col}` AS event_type,
                    {date_select} AS event_date
                FROM `{table_name}`
                ORDER BY event_date DESC
                LIMIT :limit
                """
            )
            try:
                with self.final_engine.connect() as conn:
                    rows = conn.execute(sql, {"limit": int(limit)}).mappings().all()
                return [
                    {
                        "table_name": row.get("table_name") or "",
                        "field_name": row.get("field_name") or "",
                        "event_type": self._normalize_audit_event(
                            row.get("event_type")
                        ),
                        "date": self._iso(row.get("event_date")),
                    }
                    for row in rows
                ]
            except SQLAlchemyError as exc:
                logger.error("Erro ao consultar auditoria em %s: %s", table_name, exc)
        return []

    def _count_first_existing_table(self, *table_names: str) -> int:
        for table_name in table_names:
            if not self._table_exists(Settings.DB_NAME, table_name):
                continue
            try:
                with self.final_engine.connect() as conn:
                    self._prepare_metrics_session(conn)
                    value = conn.execute(
                        text(f"SELECT COUNT(*) FROM `{table_name}`")
                    ).scalar()
                return int(value or 0)
            except SQLAlchemyError as exc:
                self._handle_sqlalchemy_error(
                    exc,
                    "Erro ao contar tabela %s",
                    table_name,
                )
                return 0
        return 0

    def _database_count_tables(self) -> Dict[str, tuple[str, ...]]:
        return {
            "empresa_total": ("empresa",),
            "estabelecimento_total": ("estabelecimento",),
            "socio_total": ("socio",),
            "cnae_total": ("cnae",),
            "municipio_total": ("municipio", "munic"),
        }

    def _empty_database_counts(self) -> Dict[str, int]:
        return {key: 0 for key in self._database_count_tables()}

    def _estimated_database_counts(self) -> Optional[Dict[str, int]]:
        counts = self._empty_database_counts()
        aliases = self._database_count_tables()
        table_names = sorted({name for names in aliases.values() for name in names})

        try:
            with self.final_engine.connect() as conn:
                self._prepare_metrics_session(conn)
                rows = conn.execute(
                    text(
                        """
                        SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 0) AS table_rows
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = :schema_name
                          AND TABLE_NAME IN :table_names
                        """
                    ).bindparams(bindparam("table_names", expanding=True)),
                    {
                        "schema_name": Settings.DB_NAME,
                        "table_names": table_names,
                    },
                ).mappings().all()

            rows_by_table = {
                str(row["TABLE_NAME"]): int(row["table_rows"] or 0)
                for row in rows
            }
            for metric_name, candidates in aliases.items():
                for table_name in candidates:
                    if table_name in rows_by_table:
                        counts[metric_name] = rows_by_table[table_name]
                        break
            return counts
        except SQLAlchemyError as exc:
            self._handle_sqlalchemy_error(
                exc,
                "Erro ao estimar contagens do schema %s",
                Settings.DB_NAME,
            )
            return None

    def _latest_promotion_duration(self) -> float:
        db = self.ops_session_factory()
        try:
            row = (
                db.query(ETLRunPhase)
                .filter(ETLRunPhase.phase_name == "PROMOTE_RAW_IMPORT")
                .order_by(ETLRunPhase.started_at.desc())
                .first()
            )
            return float(row.duration_seconds or 0) if row else 0
        finally:
            db.close()

    def _latest_metric_value(self, rows: Iterable[ETLMetric], marker: str) -> float:
        for row in rows:
            if marker in (row.metric_name or ""):
                return float(row.metric_value or 0)
        return 0

    def _table_exists(self, schema_name: str, table_name: str) -> bool:
        cache_key = f"{schema_name}.{table_name}"
        if cache_key in self._table_exists_cache:
            return self._table_exists_cache[cache_key]

        try:
            with self.final_engine.connect() as conn:
                self._prepare_metrics_session(conn)
                row = conn.execute(
                    text(
                        """
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = :schema_name
                          AND TABLE_NAME = :table_name
                        """
                    ),
                    {"schema_name": schema_name, "table_name": table_name},
                ).first()
            exists = row is not None
            self._table_exists_cache[cache_key] = exists
            return exists
        except SQLAlchemyError as exc:
            self._handle_sqlalchemy_error(
                exc,
                "Erro ao verificar tabela %s.%s",
                schema_name,
                table_name,
            )
            return False

    def _table_columns(self, schema_name: str, table_name: str) -> List[str]:
        try:
            with self.final_engine.connect() as conn:
                self._prepare_metrics_session(conn)
                rows = conn.execute(
                    text(
                        """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = :schema_name
                          AND TABLE_NAME = :table_name
                        """
                    ),
                    {"schema_name": schema_name, "table_name": table_name},
                ).scalars().all()
            return [str(row) for row in rows]
        except SQLAlchemyError as exc:
            self._handle_sqlalchemy_error(
                exc,
                "Erro ao consultar colunas de %s.%s",
                schema_name,
                table_name,
            )
            return []

    def _prepare_metrics_session(self, conn) -> None:
        timeout = max(1, Settings.METRICS_DB_QUERY_TIMEOUT)
        try:
            conn.execute(text(f"SET SESSION lock_wait_timeout = {timeout}"))
            conn.execute(text(f"SET SESSION wait_timeout = {timeout + 5}"))
            conn.execute(text(f"SET SESSION net_read_timeout = {timeout}"))
            conn.execute(text(f"SET SESSION net_write_timeout = {timeout}"))
        except SQLAlchemyError:
            logger.debug("Nao foi possivel ajustar timeouts da sessao de metricas")

    def _handle_sqlalchemy_error(self, exc: SQLAlchemyError, message: str, *args: Any) -> None:
        if is_database_connection_lost(exc):
            self.final_engine.dispose()
            logger.warning(message + ": conexao perdida com MySQL; pool reiniciado", *args)
            return

        logger.error(message + ": %s", *args, exc, exc_info=True)

    def _first_existing(self, columns: Iterable[str], *candidates: str) -> Optional[str]:
        available = {column.lower(): column for column in columns}
        for candidate in candidates:
            if candidate.lower() in available:
                return available[candidate.lower()]
        return None

    def _promotion_strategy(self, row: Dict[str, Any]) -> str:
        text_value = (row.get("alteracao") or "").lower()
        if "rename_swap" in text_value:
            return "rename_swap"
        return Settings.DB_PROMOTION_STRATEGY

    def _normalize_audit_event(self, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"added", "novo", "campo_novo", "new"}:
            return "added"
        if normalized in {"removed", "removido", "campo_removido", "deleted"}:
            return "removed"
        if normalized in {"changed", "alterado", "campo_alterado", "updated"}:
            return "changed"
        return normalized or "unknown"

    def _combine_date_time(self, created_at: Any, movement_date: Any, movement_time: Any) -> Optional[str]:
        if created_at:
            return self._iso(created_at)
        if isinstance(movement_date, date):
            if isinstance(movement_time, time):
                return datetime.combine(movement_date, movement_time).isoformat()
            return movement_date.isoformat()
        return self._iso(movement_date)

    def _iso(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()
        return str(value)

    def _empty_overview(self) -> Dict[str, Any]:
        return {
            "last_execution": None,
            "status": "UNKNOWN",
            "total_duration_seconds": 0,
            "files_processed": 0,
            "records_loaded": 0,
            "errors": 0,
        }
