from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from app.models import Empresa
from app.exceptions import (
    DuplicateEntityError,
    DatabaseError,
    NotFoundError
)
import logging

logger = logging.getLogger(__name__)


class EmpresaRepository:

    def __init__(self, db: Session):
        self.db = db

    def create(self, empresa: Empresa):
        try:
            self.db.add(empresa)
            self.db.commit()
            self.db.refresh(empresa)

            logger.info(f"Empresa criada: {empresa.cnpj_basico}")
            return empresa

        except IntegrityError:
            self.db.rollback()

            logger.warning(f"CNPJ duplicado: {empresa.cnpj_basico}")

            raise DuplicateEntityError(
                f"Já existe uma empresa com CNPJ {empresa.cnpj_basico}"
            )

        except SQLAlchemyError as e:
            self.db.rollback()

            logger.error(f"Erro ao inserir empresa: {e}")

            raise DatabaseError("Erro ao inserir empresa no banco")

    def get_by_cnpj(self, cnpj: str):
        empresa = self.db.query(Empresa).filter(
            Empresa.cnpj_basico == cnpj
        ).first()

        if not empresa:
            raise NotFoundError(f"Empresa {cnpj} não encontrada")

        return empresa

    def get_all(self, limit: int = 100):
        return self.db.query(Empresa).limit(limit).all()

    def update(self, cnpj: str, data: dict):
        try:
            empresa = self.get_by_cnpj(cnpj)

            for key, value in data.items():
                if hasattr(empresa, key):
                    setattr(empresa, key, value)

            self.db.commit()
            self.db.refresh(empresa)

            logger.info(f"Empresa atualizada: {cnpj}")
            return empresa

        except NotFoundError:
            raise

        except SQLAlchemyError as e:
            self.db.rollback()

            logger.error(f"Erro ao atualizar empresa {cnpj}: {e}")
            raise DatabaseError("Erro ao atualizar empresa")

    def delete(self, cnpj: str):
        try:
            empresa = self.get_by_cnpj(cnpj)

            self.db.delete(empresa)
            self.db.commit()

            logger.info(f"Empresa removida: {cnpj}")
            return True

        except NotFoundError:
            raise

        except SQLAlchemyError as e:
            self.db.rollback()

            logger.error(f"Erro ao deletar empresa {cnpj}: {e}")
            raise DatabaseError("Erro ao remover empresa")