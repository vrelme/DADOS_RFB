import os

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

    DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

    # =====================================================
    # DATABASE URL
    # =====================================================
    DATABASE_URL = (
        f"{DB_DRIVER}://"
        f"{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
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

        cls.ERROR_DIR.mkdir(parents=True, exist_ok=True)

        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
