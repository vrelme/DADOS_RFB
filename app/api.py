from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.cnpj_api import CNPJConsulta, CNPJRepository
from app.database import SessionLocal
from app.exceptions import DatabaseError, ValidationError


app = FastAPI(
    title="API Dados RFB",
    version="1.0.0",
    description="Consulta de situacao cadastral e logradouro por CNPJ.",
)


class CNPJRequest(BaseModel):
    cnpj: str = Field(..., examples=["12.345.678/0001-95"])


class CNPJResponse(BaseModel):
    cnpj: str
    ativo: bool
    logradouro: Optional[str] = None
    situacao_cadastral: Optional[str] = None


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _to_response(result: CNPJConsulta) -> CNPJResponse:
    return CNPJResponse(
        cnpj=result.cnpj,
        ativo=result.ativo,
        logradouro=result.logradouro,
        situacao_cadastral=result.situacao_cadastral,
    )


def _consultar_cnpj(cnpj: str, db: Session):
    try:
        result = CNPJRepository(db).get_by_cnpj(cnpj)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="CNPJ nao encontrado",
        )

    return _to_response(result)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/cnpj/{cnpj}", response_model=CNPJResponse)
def consultar_cnpj(cnpj: str, db: Session = Depends(get_db)):
    return _consultar_cnpj(cnpj, db)


@app.post("/api/v1/cnpj", response_model=CNPJResponse)
def consultar_cnpj_por_post(
    payload: CNPJRequest,
    db: Session = Depends(get_db),
):
    return _consultar_cnpj(payload.cnpj, db)
