import logging

from app.database import is_database_connection_lost
from app.main import execute_with_database_recovery


def test_connection_loss_is_found_in_exception_context():
    try:
        raise ConnectionResetError(
            "Lost connection to MySQL server during query"
        )
    except ConnectionResetError:
        try:
            raise AttributeError(
                "'NoneType' object has no attribute 'settimeout'"
            )
        except AttributeError as error:
            assert is_database_connection_lost(error)


def test_execute_waits_for_database_after_pymysql_socket_error(monkeypatch):
    attempts = []
    waits = []

    def operation():
        attempts.append(1)
        if len(attempts) == 1:
            raise AttributeError(
                "'NoneType' object has no attribute 'settimeout'"
            )
        return "ok"

    monkeypatch.setattr(
        "app.main.wait_for_database_recovery",
        lambda logger, context: waits.append(context),
    )

    result = execute_with_database_recovery(
        logging.getLogger("test_recovery"),
        "run_etl",
        operation,
    )

    assert result == "ok"
    assert len(attempts) == 2
    assert waits == ["operacao=run_etl"]
