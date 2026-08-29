import re
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.exceptions import DatabaseError, ValidationError
from app.models import Estabelecimento


CNPJ_LENGTH = 14
ACTIVE_SITUACAO_CADASTRAL = "02"


@dataclass(frozen=True)
class CNPJParts:
    completo: str
    basico: str
    ordem: str
    dv: str


@dataclass(frozen=True)
class CNPJConsulta:
    cnpj: str
    ativo: bool
    logradouro: Optional[str]
    situacao_cadastral: Optional[str]


def normalize_cnpj(cnpj: str) -> CNPJParts:
    digits = re.sub(r"\D", "", str(cnpj or ""))

    if len(digits) != CNPJ_LENGTH:
        raise ValidationError("CNPJ deve conter 14 digitos")

    return CNPJParts(
        completo=digits,
        basico=digits[:8],
        ordem=digits[8:12],
        dv=digits[12:],
    )


def build_logradouro(tipo_logradouro: Optional[str], logradouro: Optional[str]):
    parts = [
        part.strip()
        for part in (tipo_logradouro, logradouro)
        if part and part.strip()
    ]
    return " ".join(parts) or None


class CNPJRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_cnpj(self, cnpj: str) -> Optional[CNPJConsulta]:
        parts = normalize_cnpj(cnpj)

        stmt = (
            select(
                Estabelecimento.situacao_cadastral,
                Estabelecimento.tipo_logradouro,
                Estabelecimento.logradouro,
            )
            .where(Estabelecimento.cnpj_basico == parts.basico)
            .where(Estabelecimento.cnpj_ordem == parts.ordem)
            .where(Estabelecimento.cnpj_dv == parts.dv)
            .limit(1)
        )

        try:
            row = self.db.execute(stmt).mappings().first()
        except SQLAlchemyError as exc:
            raise DatabaseError("Erro ao consultar CNPJ no banco") from exc

        if not row:
            return None

        situacao = row["situacao_cadastral"]
        return CNPJConsulta(
            cnpj=parts.completo,
            ativo=situacao == ACTIVE_SITUACAO_CADASTRAL,
            logradouro=build_logradouro(
                row["tipo_logradouro"],
                row["logradouro"],
            ),
            situacao_cadastral=situacao,
        )
