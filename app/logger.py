import logging
import textwrap
from logging.handlers import RotatingFileHandler
from app.config import Settings


class MaxLineLengthFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, max_line_length=140):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.max_line_length = max_line_length

    def format(self, record):
        formatted = super().format(record)
        return "\n".join(
            self._wrap_line(line)
            for line in formatted.splitlines()
        )

    def _wrap_line(self, line):
        if len(line) <= self.max_line_length:
            return line

        prefix, message = self._split_prefix(line)
        width = max(20, self.max_line_length - len(prefix))
        wrapped = textwrap.wrap(
            message,
            width=width,
            break_long_words=True,
            break_on_hyphens=False,
        ) or [""]
        return "\n".join(f"{prefix}{part}" for part in wrapped)

    def _split_prefix(self, line):
        parts = line.split(" | ", 2)
        if len(parts) != 3:
            return "", line
        return f"{parts[0]} | {parts[1]} | ", parts[2]


def setup_logger():
    Settings.LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = MaxLineLengthFormatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        max_line_length=140,
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

    error_detail_logger = logging.getLogger("error_detail")
    error_detail_logger.setLevel(logging.INFO)
    error_detail_logger.handlers.clear()
    error_detail_logger.propagate = False

    error_handler = RotatingFileHandler(
        Settings.LOG_DIR / "error.log",
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    error_handler.setFormatter(formatter)
    error_detail_logger.addHandler(error_handler)

    return logger
