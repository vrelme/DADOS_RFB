from app.database import SessionLocal
from app.models import Empresa
from app.repositories import EmpresaRepository
from app.exceptions import AppError
import logging

logger = logging.getLogger(__name__)


class EmpresaService:

    @staticmethod
    def create_empresa(data: dict):
        db = SessionLocal()
        try:
            repo = EmpresaRepository(db)
            empresa = Empresa(**data)
            return repo.create(empresa)

        except AppError as e:
            logger.warning(str(e))
            raise

        finally:
            db.close()

    @staticmethod
    def get_empresa(cnpj: str):
        db = SessionLocal()
        try:
            repo = EmpresaRepository(db)
            return repo.get_by_cnpj(cnpj)

        finally:
            db.close()

    @staticmethod
    def update_empresa(cnpj: str, data: dict):
        db = SessionLocal()
        try:
            repo = EmpresaRepository(db)
            return repo.update(cnpj, data)

        finally:
            db.close()

    @staticmethod
    def delete_empresa(cnpj: str):
        db = SessionLocal()
        try:
            repo = EmpresaRepository(db)
            return repo.delete(cnpj)

        finally:
            db.close()