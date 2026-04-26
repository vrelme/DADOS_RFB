import logging
from logging.handlers import RotatingFileHandler
from src import config

def setup_logger():
    config.LOG_DIR.mkdir(exist_ok=True)

    file_handler = RotatingFileHandler(
        config.LOG_DIR / "app.log",
        maxBytes=5_000_000,
        backupCount=5
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[file_handler]
    )