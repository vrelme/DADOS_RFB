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


def test_root_logger_errors_are_written_to_error_log(tmp_path, monkeypatch):
    import app.logger as logger_module
    from app.config import Settings

    monkeypatch.setattr(Settings, "LOG_DIR", tmp_path / "logs", raising=False)
    monkeypatch.setattr(Settings, "LOG_LEVEL", "INFO", raising=False)

    logger_module.setup_logger()

    logging.getLogger().error("simulated failure for error log")

    error_log_path = tmp_path / "logs" / "error.log"

    assert error_log_path.exists()
    content = error_log_path.read_text(encoding="utf-8")
    assert "simulated failure for error log" in content


def test_app_log_replaces_error_details_with_single_notice(tmp_path, monkeypatch):
    import app.logger as logger_module
    from app.config import Settings

    monkeypatch.setattr(Settings, "LOG_DIR", tmp_path / "logs", raising=False)
    monkeypatch.setattr(Settings, "LOG_LEVEL", "INFO", raising=False)

    logger = logger_module.setup_logger()
    try:
        logger.error("sensitive database failure")
        logger.error("sensitive traceback")

        app_content = (tmp_path / "logs" / "app.log").read_text(encoding="utf-8")
        error_content = (tmp_path / "logs" / "error.log").read_text(encoding="utf-8")

        assert app_content.count(logger_module.AppLogFormatter.ERROR_NOTICE) == 1
        assert "sensitive database failure" not in app_content
        assert "sensitive traceback" not in app_content
        assert "sensitive database failure" in error_content
        assert "sensitive traceback" in error_content
    finally:
        for handler in logger.handlers:
            handler.close()


def test_app_log_removes_sqlalchemy_details_from_warning():
    from app.logger import AppLogFormatter

    formatter = AppLogFormatter("%(levelname)s | %(message)s")
    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg=(
            "Falha ao gravar checkpoint ETL: conexao perdida\n"
            "[SQL: SELECT * FROM etl_checkpoint]\n"
            "[parameters: {'file_name': 'arquivo.csv'}]\n"
            "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
        ),
        args=(),
        exc_info=None,
    )

    content = formatter.format(record)

    assert "Falha ao gravar checkpoint ETL: conexao perdida" in content
    assert "[SQL:" not in content
    assert "[parameters:" not in content
    assert "Background on this error" not in content
