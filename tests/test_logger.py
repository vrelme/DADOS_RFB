import importlib
import logging

from app.logger import MaxLineLengthFormatter


def test_formatter_wraps_long_log_lines_to_140_chars():
    formatter = MaxLineLengthFormatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        max_line_length=140,
    )
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="socio | ordem de carga | " + ", ".join(
            f"K3241.K03200Y{i}.D60509.SOCIOCSV" for i in range(10)
        ),
        args=(),
        exc_info=None,
    )

    lines = formatter.format(record).splitlines()

    assert len(lines) > 1
    assert all(len(line) <= 140 for line in lines)
    assert any("INFO" in line for line in lines)
    assert any(" | " in line and " | " in line[1:] for line in lines)


def test_logger_setup_initializes_root_logger_on_import():
    import app.logger as logger_module

    importlib.reload(logger_module)

    root_logger = logging.getLogger()

    assert root_logger.level == logging.INFO
    assert any(isinstance(handler, logging.StreamHandler) for handler in root_logger.handlers)
