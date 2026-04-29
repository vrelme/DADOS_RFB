import logging
from logging.handlers import RotatingFileHandler
from app.config import Settings

def setup_logger():
    Settings.LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        Settings.LOG_DIR / "app.log",
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger