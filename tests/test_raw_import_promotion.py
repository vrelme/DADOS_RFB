from types import SimpleNamespace

from app.etl import raw_import_promotion
from app.etl.raw_import_promotion import RawImportPromotionRepository


class _NoopBegin:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class _NoopEngine:
    def begin(self):
        return _NoopBegin()


def _repository_for_swap_test(table_exists):
    repo = object.__new__(RawImportPromotionRepository)
    repo.import_db = "rfb_import"
    repo.final_db = "dados_rfb"
    repo.final_engine = _NoopEngine()
    repo.table_timings = {}
    repo.monitor_calls = []
    repo.swap_calls = []
    repo.control_rows = []

    repo._table_exists = lambda database_name, table_name: table_exists
    repo._monitor_business_changes = lambda table_name: repo.monitor_calls.append(table_name)
    repo._swap_table_to_final = lambda table_name: repo.swap_calls.append(table_name)
    repo._insert_control = (
        lambda conn, table_name, status, detail: repo.control_rows.append(
            (table_name, status, detail)
        )
    )
    return repo


def test_rename_swap_skips_monitoring_when_final_table_does_not_exist(monkeypatch):
    table = SimpleNamespace(table_name="empresa")
    monkeypatch.setattr(raw_import_promotion, "RFB_TABLES", [table])
    repo = _repository_for_swap_test(table_exists=False)

    repo._swap_all_tables_to_final()

    assert repo.monitor_calls == []
    assert repo.swap_calls == ["empresa"]
    assert repo.control_rows[0][1] == "copiada"
    assert "monitoracao foi ignorada" in repo.control_rows[0][2]


def test_rename_swap_monitors_when_final_table_exists(monkeypatch):
    table = SimpleNamespace(table_name="empresa")
    monkeypatch.setattr(raw_import_promotion, "RFB_TABLES", [table])
    repo = _repository_for_swap_test(table_exists=True)

    repo._swap_all_tables_to_final()

    assert repo.monitor_calls == ["empresa"]
    assert repo.swap_calls == ["empresa"]
    assert repo.control_rows[0][1] == "promovida"
