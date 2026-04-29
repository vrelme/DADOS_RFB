import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Settings:
    APP_NAME = os.getenv("APP_NAME", "RFB Loader")
    APP_ENV = os.getenv("APP_ENV", "dev")

    DB_TYPE = os.getenv("DB_TYPE", "mysql")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "rfb")

    INPUT_DIR = Path(os.getenv("INPUT_DIR", "data/input"))
    EXTRACT_DIR = Path(os.getenv("EXTRACT_DIR", "data/extracted"))
    LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))