import logging
import textwrap
from logging.handlers import RotatingFileHandler
from app.config import Settings


class MaxLineLengthFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, max_line_length=140):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.max_line_length = max_line_length

    def format(self, record):
        original_levelname = record.levelname
        record.levelname = f"{record.levelname:^10}"
        try:
            formatted = super().format(record)
        finally:
            record.levelname = original_levelname
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


class AppLogFormatter(MaxLineLengthFormatter):
    """Oculta detalhes de erro no log geral sem alterar os outros handlers."""

    ERROR_NOTICE = "Ocorreu um erro. Consulte error.log para analisar os detalhes."

    @staticmethod
    def _without_sqlalchemy_details(message):
        for marker in (
            "\n[SQL:",
            "\n[parameters:",
            "\n(Background on this error at:",
        ):
            message = message.split(marker, 1)[0]
        return message.rstrip()

    def format(self, record):
        original_msg = record.msg
        original_args = record.args
        original_exc_info = record.exc_info
        original_exc_text = record.exc_text
        original_stack_info = record.stack_info
        try:
            if record.levelno >= logging.ERROR:
                record.msg = self.ERROR_NOTICE
                record.exc_info = None
                record.exc_text = None
                record.stack_info = None
            else:
                record.msg = self._without_sqlalchemy_details(
                    record.getMessage()
                )
            record.args = ()
            return super().format(record)
        finally:
            record.msg = original_msg
            record.args = original_args
            record.exc_info = original_exc_info
            record.exc_text = original_exc_text
            record.stack_info = original_stack_info


class CollapseAppLogErrors(logging.Filter):
    """Grava somente o primeiro registro de cada bloco consecutivo de erros."""

    def __init__(self):
        super().__init__()
        self._error_block_active = False

    def filter(self, record):
        if record.levelno >= logging.ERROR:
            if self._error_block_active:
                return False
            self._error_block_active = True
            return True

        self._error_block_active = False
        return True


def setup_logger():
    logging.addLevelName(logging.INFO, "INFO")
    logging.addLevelName(logging.WARNING, "AVISO")
    logging.addLevelName(logging.ERROR, "ERRO")
    logging.addLevelName(logging.CRITICAL, "CRÍTICO")

    Settings.LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, Settings.LOG_LEVEL.upper(), logging.INFO))
    logger.handlers.clear()

    formatter = MaxLineLengthFormatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        max_line_length=140,
    )
    app_formatter = AppLogFormatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        max_line_length=140,
    )

    file_handler = RotatingFileHandler(
        Settings.LOG_DIR / "app.log",
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8"
    )

    file_handler.setFormatter(app_formatter)
    file_handler.addFilter(CollapseAppLogErrors())
    logger.addHandler(file_handler)

    error_file_handler = RotatingFileHandler(
        Settings.LOG_DIR / "error.log",
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)

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
