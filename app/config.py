from pathlib import Path
from dotenv import load_dotenv
import os

# carrega .env
load_dotenv()


class Settings:
    """
    Configurações centrais do projeto
    """

    # =========================
    # BASE PATH
    # =========================
    BASE_DIR = Path(__file__).resolve().parent.parent

    # =========================
    # DATABASE
    # =========================
    DB_DRIVER = os.getenv("DB_DRIVER", "mysql+pymysql")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "rfb_loader")

    # =========================
    # DIRECTORIES
    # =========================
    DATA_DIR = BASE_DIR / "data"
    INPUT_DIR = Path(
        os.getenv("INPUT_DIR", r"D:/OPERACAO/EXTRACTED_FILES")
    )
    PROCESSED_DIR = Path(
        os.getenv("PROCESSED_DIR", r"D:/operacao/processed")
    )

    ERROR_DIR = Path(
        os.getenv("ERROR_DIR", r"D:/operacao/error")
    )
    
    LOG_DIR = BASE_DIR / "logs"

    # =========================
    # ETL
    # =========================
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 50000))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))

    # =========================
    # APP
    # =========================
    APP_NAME = "RFB Loader Enterprise v4"
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"

    # =========================
    # SQLALCHEMY URL
    # =========================
    DATABASE_URL = (
        f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    # =========================
    # Multiprocessing
    # =========================
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))