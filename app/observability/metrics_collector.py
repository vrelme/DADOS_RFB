import logging
import threading
import time
from datetime import datetime
from typing import Dict

from app.config import Settings
from app.observability.metrics_repository import MetricsRepository
from app.observability import prometheus_metrics as metrics


logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, repository: MetricsRepository | None = None, interval: int | None = None):
        self.repository = repository or MetricsRepository()
        self.interval = int(interval or Settings.METRICS_COLLECTION_INTERVAL)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="rfb-metrics-collector",
            daemon=True,
        )
        self._thread.start()
        logger.info("Metrics collector iniciado com intervalo=%ss", self.interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def collect_once(self) -> None:
        with self._lock:
            started_at = time.time()
            try:
                self._collect()
                metrics.COLLECTION_DURATION_SECONDS.observe(time.time() - started_at)
            except Exception as exc:
                metrics.COLLECTION_ERRORS_TOTAL.inc()
                logger.error("Erro ao coletar metricas: %s", exc, exc_info=True)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.collect_once()
            self._stop_event.wait(self.interval)

    def _collect(self) -> None:
        metrics.APP_INFO.info(
            {
                "app_name": Settings.APP_NAME,
                "app_version": Settings.APP_VERSION,
                "environment": Settings.ENVIRONMENT,
                "database": Settings.DB_NAME,
                "operational_database": Settings.OPERATIONAL_DB_NAME,
            }
        )
        self._collect_overview()
        self._collect_performance()
        self._collect_database()
        self._collect_execution()
        self._collect_promotion()
        self._collect_audit()

    def _collect_overview(self) -> None:
        overview = self.repository.get_overview_metrics()
        last_execution = overview.get("last_execution")
        metrics.ETL_LAST_EXECUTION_TIMESTAMP.set(self._timestamp(last_execution))
        metrics.ETL_LAST_EXECUTION_DURATION_SECONDS.set(
            overview.get("total_duration_seconds") or 0
        )
        metrics.ETL_FILES_PROCESSED_TOTAL.set(overview.get("files_processed") or 0)
        metrics.ETL_RECORDS_PROCESSED_TOTAL.set(overview.get("records_loaded") or 0)
        metrics.ETL_ERRORS_TOTAL.set(overview.get("errors") or 0)

        status = str(overview.get("status") or "UNKNOWN").upper()
        for known_status in ("SUCCESS", "RUNNING", "STARTED", "FAILED", "UNKNOWN"):
            value = 1 if status == known_status else 0
            metrics.ETL_LAST_EXECUTION_STATUS.labels(status=known_status).set(value)

    def _collect_performance(self) -> None:
        performance = self.repository.get_performance_metrics()
        labels = self._labels(
            pipeline="RFB_LOADER_ENTERPRISE",
            file_name="",
            table_name="",
            stage="pipeline",
            status="latest",
        )
        metrics.ETL_RECORDS_PER_SECOND.labels(**labels).set(
            performance.get("records_per_second") or 0
        )
        metrics.ETL_MERGE_DURATION_SECONDS.labels(**labels).set(
            performance.get("merge_duration_seconds") or 0
        )
        metrics.ETL_RENAME_SWAP_DURATION_SECONDS.labels(**labels).set(
            performance.get("rename_swap_duration_seconds") or 0
        )

        for item in performance.get("file_durations", []):
            item_labels = self._labels(
                pipeline=item.get("pipeline"),
                file_name=item.get("file_name"),
                table_name=item.get("table_name"),
                stage=item.get("stage"),
                status=item.get("status"),
            )
            metrics.ETL_FILE_DURATION_SECONDS.labels(**item_labels).set(
                item.get("duration_seconds") or 0
            )
            metrics.ETL_RECORDS_PER_SECOND.labels(**item_labels).set(
                item.get("records_per_second") or 0
            )

        for item in performance.get("stage_durations", []):
            item_labels = self._labels(
                pipeline=item.get("pipeline"),
                file_name=item.get("file_name"),
                table_name=item.get("table_name"),
                stage=item.get("stage"),
                status=item.get("status"),
            )
            metrics.ETL_STAGE_DURATION_SECONDS.labels(**item_labels).set(
                item.get("duration_seconds") or 0
            )

    def _collect_database(self) -> None:
        counts = self.repository.get_database_counts()
        metrics.DB_EMPRESA_TOTAL.set(counts.get("empresa_total") or 0)
        metrics.DB_ESTABELECIMENTO_TOTAL.set(
            counts.get("estabelecimento_total") or 0
        )
        metrics.DB_SOCIO_TOTAL.set(counts.get("socio_total") or 0)
        metrics.DB_CNAE_TOTAL.set(counts.get("cnae_total") or 0)
        metrics.DB_MUNICIPIO_TOTAL.set(counts.get("municipio_total") or 0)

    def _collect_execution(self) -> None:
        history = self.repository.get_execution_history(limit=100)
        for item in history.get("executions", []):
            labels = self._labels(
                pipeline=item.get("pipeline"),
                worker=item.get("worker"),
                file_name=item.get("file_name"),
                status=item.get("status"),
                table_name=item.get("table_name"),
            )
            metrics.ETL_EXECUTION_TOTAL.labels(**labels).set(1)
            metrics.ETL_EXECUTION_DURATION_SECONDS.labels(**labels).set(
                item.get("duration_seconds") or 0
            )
            metrics.ETL_EXECUTION_RECORDS_TOTAL.labels(**labels).set(
                item.get("records") or 0
            )
            metrics.ETL_EXECUTION_STATUS_TOTAL.labels(**labels).set(1)

    def _collect_promotion(self) -> None:
        data = self.repository.get_promotion_metrics(limit=100)
        totals: Dict[tuple, int] = {}
        for item in data.get("promotions", []):
            key = (
                item.get("table_name") or "",
                item.get("strategy") or "",
                item.get("status") or "",
            )
            totals[key] = totals.get(key, 0) + 1

            labels = {
                "table_name": key[0],
                "strategy": key[1],
                "status": key[2],
            }
            metrics.PROMOTION_DURATION_SECONDS.labels(**labels).set(
                item.get("duration_seconds") or 0
            )

        for (table_name, strategy, status), total in totals.items():
            labels = {
                "table_name": table_name,
                "strategy": strategy,
                "status": status,
            }
            metrics.PROMOTION_TOTAL.labels(**labels).set(total)
            metrics.PROMOTION_STATUS_TOTAL.labels(**labels).set(total)
            if strategy == "rename_swap":
                metrics.PROMOTION_RENAME_SWAP_TOTAL.labels(**labels).set(total)

    def _collect_audit(self) -> None:
        audit = self.repository.get_audit_metrics(limit=100)
        history = audit.get("history") or [
            {"table_name": "", "field_name": "", "event_type": "added"},
            {"table_name": "", "field_name": "", "event_type": "removed"},
            {"table_name": "", "field_name": "", "event_type": "changed"},
        ]

        for item in history:
            labels = {
                "table_name": item.get("table_name") or "",
                "field_name": item.get("field_name") or "",
                "event_type": item.get("event_type") or "unknown",
            }
            metrics.AUDIT_EVENTS_TOTAL.labels(**labels).set(1)
            if labels["event_type"] == "added":
                metrics.AUDIT_FIELDS_ADDED_TOTAL.labels(**labels).set(
                    audit.get("fields_added") or 0
                )
            elif labels["event_type"] == "removed":
                metrics.AUDIT_FIELDS_REMOVED_TOTAL.labels(**labels).set(
                    audit.get("fields_removed") or 0
                )
            elif labels["event_type"] == "changed":
                metrics.AUDIT_FIELDS_CHANGED_TOTAL.labels(**labels).set(
                    audit.get("fields_changed") or 0
                )

    def _labels(self, **values: object) -> Dict[str, str]:
        return {key: str(value or "") for key, value in values.items()}

    def _timestamp(self, value: object) -> float:
        if not value:
            return 0
        if isinstance(value, datetime):
            return value.timestamp()
        try:
            return datetime.fromisoformat(str(value)).timestamp()
        except ValueError:
            return 0
