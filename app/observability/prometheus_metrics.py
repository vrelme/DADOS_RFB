from prometheus_client import Counter, Gauge, Histogram, Info


APP_INFO = Info(
    "rfb_loader_enterprise",
    "RFB Loader Enterprise application information",
)

ETL_LAST_EXECUTION_TIMESTAMP = Gauge(
    "rfb_etl_last_execution_timestamp",
    "Unix timestamp of the latest ETL execution",
)
ETL_LAST_EXECUTION_STATUS = Gauge(
    "rfb_etl_last_execution_status",
    "Latest ETL execution status encoded as 1=success, 0=running, -1=failed",
    ["status"],
)
ETL_LAST_EXECUTION_DURATION_SECONDS = Gauge(
    "rfb_etl_last_execution_duration_seconds",
    "Duration in seconds of the latest ETL execution",
)
ETL_FILES_PROCESSED_TOTAL = Gauge(
    "rfb_etl_files_processed_total",
    "Files processed in the latest ETL run",
)
ETL_RECORDS_PROCESSED_TOTAL = Gauge(
    "rfb_etl_records_processed_total",
    "Records processed in the latest ETL run",
)
ETL_ERRORS_TOTAL = Gauge(
    "rfb_etl_errors_total",
    "Errors in the latest ETL run",
)

ETL_RECORDS_PER_SECOND = Gauge(
    "rfb_etl_records_per_second",
    "ETL throughput in records per second",
    ["pipeline", "file_name", "table_name", "stage", "status"],
)
ETL_FILE_DURATION_SECONDS = Gauge(
    "rfb_etl_file_duration_seconds",
    "ETL file processing duration in seconds",
    ["pipeline", "file_name", "table_name", "stage", "status"],
)
ETL_STAGE_DURATION_SECONDS = Gauge(
    "rfb_etl_stage_duration_seconds",
    "ETL stage duration in seconds",
    ["pipeline", "file_name", "table_name", "stage", "status"],
)
ETL_MERGE_DURATION_SECONDS = Gauge(
    "rfb_etl_merge_duration_seconds",
    "ETL merge duration in seconds",
    ["pipeline", "file_name", "table_name", "stage", "status"],
)
ETL_RENAME_SWAP_DURATION_SECONDS = Gauge(
    "rfb_etl_rename_swap_duration_seconds",
    "ETL rename swap duration in seconds",
    ["pipeline", "file_name", "table_name", "stage", "status"],
)

DB_EMPRESA_TOTAL = Gauge("rfb_db_empresa_total", "Total rows in empresa")
DB_ESTABELECIMENTO_TOTAL = Gauge(
    "rfb_db_estabelecimento_total",
    "Total rows in estabelecimento",
)
DB_SOCIO_TOTAL = Gauge("rfb_db_socio_total", "Total rows in socio")
DB_CNAE_TOTAL = Gauge("rfb_db_cnae_total", "Total rows in cnae")
DB_MUNICIPIO_TOTAL = Gauge("rfb_db_municipio_total", "Total rows in municipio")

ETL_EXECUTION_TOTAL = Gauge(
    "rfb_etl_execution_total",
    "Total ETL executions",
    ["pipeline", "worker", "file_name", "status", "table_name"],
)
ETL_EXECUTION_DURATION_SECONDS = Gauge(
    "rfb_etl_execution_duration_seconds",
    "ETL execution duration in seconds",
    ["pipeline", "worker", "file_name", "status", "table_name"],
)
ETL_EXECUTION_RECORDS_TOTAL = Gauge(
    "rfb_etl_execution_records_total",
    "ETL execution records processed",
    ["pipeline", "worker", "file_name", "status", "table_name"],
)
ETL_EXECUTION_STATUS_TOTAL = Gauge(
    "rfb_etl_execution_status_total",
    "ETL executions grouped by status",
    ["pipeline", "worker", "file_name", "status", "table_name"],
)

PROMOTION_TOTAL = Gauge(
    "rfb_promotion_total",
    "Total promotions",
    ["table_name", "strategy", "status"],
)
PROMOTION_RENAME_SWAP_TOTAL = Gauge(
    "rfb_promotion_rename_swap_total",
    "Total rename swap promotions",
    ["table_name", "strategy", "status"],
)
PROMOTION_DURATION_SECONDS = Gauge(
    "rfb_promotion_duration_seconds",
    "Promotion duration in seconds",
    ["table_name", "strategy", "status"],
)
PROMOTION_STATUS_TOTAL = Gauge(
    "rfb_promotion_status_total",
    "Promotions grouped by status",
    ["table_name", "strategy", "status"],
)

AUDIT_FIELDS_ADDED_TOTAL = Gauge(
    "rfb_audit_fields_added_total",
    "Total audit events for added fields",
    ["table_name", "field_name", "event_type"],
)
AUDIT_FIELDS_REMOVED_TOTAL = Gauge(
    "rfb_audit_fields_removed_total",
    "Total audit events for removed fields",
    ["table_name", "field_name", "event_type"],
)
AUDIT_FIELDS_CHANGED_TOTAL = Gauge(
    "rfb_audit_fields_changed_total",
    "Total audit events for changed fields",
    ["table_name", "field_name", "event_type"],
)
AUDIT_EVENTS_TOTAL = Gauge(
    "rfb_audit_events_total",
    "Total audit events",
    ["table_name", "field_name", "event_type"],
)

COLLECTION_ERRORS_TOTAL = Counter(
    "rfb_metrics_collection_errors_total",
    "Metrics collection errors",
)
COLLECTION_DURATION_SECONDS = Histogram(
    "rfb_metrics_collection_duration_seconds",
    "Metrics collection duration in seconds",
)
