import pytest

from app.cnpj_api import (
    ACTIVE_SITUACAO_CADASTRAL,
    build_logradouro,
    normalize_cnpj,
)
from app.exceptions import ValidationError


def test_normalize_cnpj_accepts_masked_value():
    parts = normalize_cnpj("12.345.678/0001-95")

    assert parts.completo == "12345678000195"
    assert parts.basico == "12345678"
    assert parts.ordem == "0001"
    assert parts.dv == "95"


def test_normalize_cnpj_rejects_invalid_length():
    with pytest.raises(ValidationError):
        normalize_cnpj("123")


def test_build_logradouro_joins_type_and_name():
    assert build_logradouro("Rua", " das Flores ") == "Rua das Flores"


def test_build_logradouro_returns_none_without_values():
    assert build_logradouro("", None) is None


def test_active_situacao_cadastral_matches_rfb_code():
    assert ACTIVE_SITUACAO_CADASTRAL == "02"
