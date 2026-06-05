import os
from datetime import datetime

from pathlib import Path
from dotenv import load_dotenv

# =====================================================
# LOAD ENV
# =====================================================
load_dotenv()


class Settings:

    # =====================================================
    # BASE
    # =====================================================
    BASE_DIR = Path(__file__).resolve().parent.parent

    # =====================================================
    # APP
    # =====================================================
    APP_NAME = os.getenv("APP_NAME", "RFB Loader Enterprise")

    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

    DEBUG = os.getenv("DEBUG", "False").lower() == "true"

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # =====================================================
    # DATABASE
    # =====================================================
    DB_DRIVER = os.getenv("DB_DRIVER", "mysql+pymysql")

    DB_HOST = os.getenv("DB_HOST", "localhost")

    DB_PORT = int(os.getenv("DB_PORT", 3306))

    DB_USER = os.getenv("DB_USER", "root")

    DB_PASSWORD = os.getenv("DB_PASSWORD", "")

    DB_NAME = os.getenv("DB_NAME", "rfb_loader")

    IMPORT_DB_NAME = os.getenv("IMPORT_DB_NAME", f"{DB_NAME}_import")

    OPERATIONAL_DB_NAME = os.getenv("OPERATIONAL_DB_NAME", f"{DB_NAME}_ops")

    SYNC_STRATEGY = os.getenv("SYNC_STRATEGY", "full_refresh").lower()

    RAW_IMPORT_STRATEGIES = {"raw_import", "import_only"}

    IMPORT_DB_PER_RUN = (
        os.getenv("IMPORT_DB_PER_RUN", "False").lower() == "true"
    )

    IMPORT_DB_RUN_ID = (
        os.getenv("IMPORT_DB_RUN_ID")
        or datetime.now().strftime("%Y%m%d_%H%M%S")
    )

    os.environ.setdefault("IMPORT_DB_RUN_ID", IMPORT_DB_RUN_ID)

    DEFAULT_ACTIVE_IMPORT_DB_NAME = (
        f"{IMPORT_DB_NAME}_{IMPORT_DB_RUN_ID}"
        if IMPORT_DB_PER_RUN
        else IMPORT_DB_NAME
    )

    if SYNC_STRATEGY in RAW_IMPORT_STRATEGIES:
        ACTIVE_DB_NAME = DEFAULT_ACTIVE_IMPORT_DB_NAME
    else:
        ACTIVE_DB_NAME = os.getenv("RFB_ACTIVE_DB_NAME", DB_NAME)

    os.environ["RFB_ACTIVE_DB_NAME"] = ACTIVE_DB_NAME

    DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

    # =====================================================
    # DATABASE URL
    # =====================================================
    DATABASE_URL = (
        f"{DB_DRIVER}://"
        f"{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{ACTIVE_DB_NAME}"
        f"?charset={DB_CHARSET}"
    )

    # =====================================================
    # ORM / SQLALCHEMY
    # =====================================================

    # Exibe SQL no terminal
    ORM_ECHO = (
        os.getenv("ORM_ECHO", "False").lower() == "true"
    )

    # SQLAlchemy 2.x
    ORM_FUTURE = (
        os.getenv("ORM_FUTURE", "True").lower() == "true"
    )

    # Session
    ORM_AUTOFLUSH = (
        os.getenv("ORM_AUTOFLUSH", "False").lower() == "true"
    )

    ORM_AUTOCOMMIT = (
        os.getenv("ORM_AUTOCOMMIT", "False").lower() == "true"
    )

    ORM_EXPIRE_ON_COMMIT = (
        os.getenv("ORM_EXPIRE_ON_COMMIT", "False").lower() == "true"
    )

    # =====================================================
    # CONNECTION POOL
    # =====================================================
    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 20))

    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", 40))

    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", 3600))

    DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", 30))

    DB_POOL_PRE_PING = (
        os.getenv("DB_POOL_PRE_PING", "True").lower() == "true"
    )

    DB_LOCAL_INFILE = (
        os.getenv("DB_LOCAL_INFILE", "True").lower() == "true"
    )

    DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", 30))

    DB_HEALTH_CHECK_TIMEOUT = int(os.getenv("DB_HEALTH_CHECK_TIMEOUT", 5))

    DB_READ_TIMEOUT = int(os.getenv("DB_READ_TIMEOUT", 600))

    DB_WRITE_TIMEOUT = int(os.getenv("DB_WRITE_TIMEOUT", 600))

    DB_PROMOTION_READ_TIMEOUT = int(
        os.getenv("DB_PROMOTION_READ_TIMEOUT", 14400)
    )

    DB_PROMOTION_WRITE_TIMEOUT = int(
        os.getenv("DB_PROMOTION_WRITE_TIMEOUT", 14400)
    )

    DB_PROMOTION_BATCH_SIZE = int(
        os.getenv("DB_PROMOTION_BATCH_SIZE", 50000)
    )

    DB_PROMOTION_LOCK_WAIT_TIMEOUT = int(
        os.getenv("DB_PROMOTION_LOCK_WAIT_TIMEOUT", 3600)
    )

    DB_PROMOTION_STRATEGY = os.getenv(
        "DB_PROMOTION_STRATEGY", "rename_swap"
    ).lower()

    DB_CREATE_RETRIES = int(os.getenv("DB_CREATE_RETRIES", 5))

    DB_CREATE_RETRY_DELAY = int(os.getenv("DB_CREATE_RETRY_DELAY", 10))

    RAW_IMPORT_RESET_TABLES = (
        os.getenv("RAW_IMPORT_RESET_TABLES", "True").lower() == "true"
    )

    RAW_IMPORT_RESET_SCHEMA = (
        os.getenv("RAW_IMPORT_RESET_SCHEMA", "True").lower() == "true"
    )

    RAW_IMPORT_FAST_SCHEMA = (
        os.getenv("RAW_IMPORT_FAST_SCHEMA", "True").lower() == "true"
    )

    PROMOTE_RAW_IMPORT_AFTER_LOAD = (
        os.getenv("PROMOTE_RAW_IMPORT_AFTER_LOAD", "True").lower() == "true"
    )

    CONTROL_DIFF_MAX_ROWS = int(os.getenv("CONTROL_DIFF_MAX_ROWS", 1000))

    CONTROL_DIFF_DETAIL_TABLES = {
        value.strip()
        for value in os.getenv(
            "CONTROL_DIFF_DETAIL_TABLES",
            "cnae,moti,munic,natju,pais,quals"
        ).split(",")
        if value.strip()
    }

    MONITORED_FIELDS_BOOTSTRAP_DEFAULTS = (
        os.getenv("MONITORED_FIELDS_BOOTSTRAP_DEFAULTS", "True").lower() == "true"
    )

    MONITORED_FIELD_BATCH_SIZE = int(
        os.getenv("MONITORED_FIELD_BATCH_SIZE", DB_PROMOTION_BATCH_SIZE)
    )

    # =====================================================
    # ETL
    # =====================================================
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 50000))

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))

    MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))

    ENABLE_PARALLELISM = (
        os.getenv("ENABLE_PARALLELISM", "True").lower() == "true"
    )

    LOAD_STRATEGY = os.getenv("LOAD_STRATEGY", "load_data").lower()

    MERGE_STRATEGY = os.getenv("MERGE_STRATEGY", "full_refresh").lower()

    # staging: carrega staging e depois faz merge; final: carrega direto nas tabelas finais.
    LOAD_TARGET = os.getenv(
        "LOAD_TARGET",
        "final" if SYNC_STRATEGY in RAW_IMPORT_STRATEGIES else "staging"
    ).lower()

    ENABLE_ADAPTIVE_WORKERS = (
        os.getenv("ENABLE_ADAPTIVE_WORKERS", "False").lower() == "true"
    )

    MIN_WORKERS = int(os.getenv("MIN_WORKERS", 1))

    ADAPTIVE_CPU_HIGH = int(os.getenv("ADAPTIVE_CPU_HIGH", 85))

    ADAPTIVE_RAM_HIGH = int(os.getenv("ADAPTIVE_RAM_HIGH", 85))

    ADAPTIVE_CPU_LOW = int(os.getenv("ADAPTIVE_CPU_LOW", 45))

    ADAPTIVE_RAM_LOW = int(os.getenv("ADAPTIVE_RAM_LOW", 65))

    AUTO_KILL_BLOCKING_SESSIONS = (
        os.getenv("AUTO_KILL_BLOCKING_SESSIONS", "False").lower() == "true"
    )

    AUTO_KILL_MIN_SECONDS = int(os.getenv("AUTO_KILL_MIN_SECONDS", 300))

    AUTO_KILL_WAIT_SECONDS = int(os.getenv("AUTO_KILL_WAIT_SECONDS", 10))

    # =====================================================
    # STAGING
    # =====================================================
    ENABLE_STAGING = (
        os.getenv("ENABLE_STAGING", "True").lower() == "true"
    )

    AUTO_TRUNCATE_STAGING = (
        os.getenv("AUTO_TRUNCATE_STAGING", "True").lower() == "true"
    )

    # =====================================================
    # PATHS
    # =====================================================
    INPUT_DIR = Path(
        os.getenv(
            "INPUT_DIR",
            BASE_DIR / "data" / "input"
        )
    )

    PROCESSED_DIR = Path(
        os.getenv(
            "PROCESSED_DIR",
            BASE_DIR / "data" / "processed"
        )
    )

    EXTRACT_DIR = Path(
        os.getenv(
            "EXTRACT_DIR",
            BASE_DIR / "data" / "extracted"
        )
    )

    ERROR_DIR = Path(
        os.getenv(
            "ERROR_DIR",
            BASE_DIR / "data" / "error"
        )
    )

    LOG_DIR = Path(
        os.getenv(
            "LOG_DIR",
            BASE_DIR / "logs"
        )
    )

    # =====================================================
    # V3 FEATURES
    # =====================================================
    ENABLE_ZIP_PROCESSING = (
        os.getenv("ENABLE_ZIP_PROCESSING", "True").lower() == "true"
    )

    ENABLE_CHECKPOINT_RESUME = (
        os.getenv("ENABLE_CHECKPOINT_RESUME", "True").lower() == "true"
    )

    ENABLE_DLQ = (
        os.getenv("ENABLE_DLQ", "True").lower() == "true"
    )

    ENABLE_DATA_QUALITY_RULES = (
        os.getenv("ENABLE_DATA_QUALITY_RULES", "True").lower() == "true"
    )

    ENABLE_OTEL_TRACING = (
        os.getenv("ENABLE_OTEL_TRACING", "False").lower() == "true"
    )

    # =====================================================
    # PERFORMANCE
    # =====================================================
    ENABLE_PERFORMANCE_LOG = (
        os.getenv("ENABLE_PERFORMANCE_LOG", "True").lower() == "true"
    )

    PERFORMANCE_LOG_INTERVAL = int(
        os.getenv("PERFORMANCE_LOG_INTERVAL", 1)
    )

    # =====================================================
    # RETRY
    # =====================================================
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

    RETRY_DELAY = int(os.getenv("RETRY_DELAY", 5))

    DB_RECOVERY_ENABLED = (
        os.getenv("DB_RECOVERY_ENABLED", "True").lower() == "true"
    )

    DB_RECOVERY_FAST_INTERVAL_SECONDS = int(
        os.getenv("DB_RECOVERY_FAST_INTERVAL_SECONDS", 5)
    )

    DB_RECOVERY_MEDIUM_INTERVAL_SECONDS = int(
        os.getenv("DB_RECOVERY_MEDIUM_INTERVAL_SECONDS", 60)
    )

    DB_RECOVERY_SLOW_INTERVAL_SECONDS = int(
        os.getenv("DB_RECOVERY_SLOW_INTERVAL_SECONDS", 300)
    )

    DB_RECOVERY_FAST_UNTIL_SECONDS = int(
        os.getenv("DB_RECOVERY_FAST_UNTIL_SECONDS", 600)
    )

    DB_RECOVERY_MEDIUM_UNTIL_SECONDS = int(
        os.getenv("DB_RECOVERY_MEDIUM_UNTIL_SECONDS", 1800)
    )

    DB_RECOVERY_MAX_WAIT_SECONDS = int(
        os.getenv("DB_RECOVERY_MAX_WAIT_SECONDS", 0)
    )

    # =====================================================
    # FILES
    # =====================================================
    FILE_ENCODING = os.getenv("FILE_ENCODING", "latin1")

    FILE_SEPARATOR = os.getenv("FILE_SEPARATOR", ";")

    # =====================================================
    # EXECUTION CONTROL
    # =====================================================
    ENABLE_EXECUTION_CONTROL = (
        os.getenv("ENABLE_EXECUTION_CONTROL", "True").lower() == "true"
    )

    # =====================================================
    # CREATE REQUIRED DIRECTORIES
    # =====================================================
    @classmethod
    def create_dirs(cls):

        cls.INPUT_DIR.mkdir(parents=True, exist_ok=True)

        cls.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        cls.EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

        cls.ERROR_DIR.mkdir(parents=True, exist_ok=True)

        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
