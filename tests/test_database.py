import importlib
import types

from app import database


def test_build_database_url_escapes_special_characters_in_credentials(monkeypatch):
    monkeypatch.setattr(database.Settings, "DB_DRIVER", "mysql+pymysql")
    monkeypatch.setattr(database.Settings, "DB_USER", "root")
    monkeypatch.setattr(database.Settings, "DB_PASSWORD", "V!da_L0ng@")
    monkeypatch.setattr(database.Settings, "DB_HOST", "192.168.1.160")
    monkeypatch.setattr(database.Settings, "DB_PORT", 3306)
    monkeypatch.setattr(database.Settings, "DB_CHARSET", "utf8mb4")
    monkeypatch.setattr(database.Settings, "ACTIVE_DB_NAME", "db_RFB")

    url = database.build_database_url()

    assert "root" in url
    assert "V%21da_L0ng%40" in url
    assert "mysql+pymysql://" in url
