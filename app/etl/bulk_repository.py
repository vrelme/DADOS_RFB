from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert
import logging

logger = logging.getLogger(__name__)


class BulkRepository:

    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # INSERT EM LOTE (ROBUSTO)
    # =====================================================
    def bulk_insert(self, model, data: list):
        """
        Insert em lote com tolerância a duplicados (INSERT IGNORE)
        Ideal para carga RFB (dados massivos e repetidos)
        """

        if not data:
            logger.debug("Nenhum dado para inserir (lista vazia)")
            return

        try:
            stmt = insert(model).values(data)

            # 🔥 IGNORA DUPLICADOS (ESSENCIAL)
            stmt = stmt.prefix_with("IGNORE")

            result = self.db.execute(stmt)
            self.db.commit()

            logger.info(
                f"{model.__tablename__}: {len(data)} registros processados"
            )

            return result

        except SQLAlchemyError as e:
            self.db.rollback()

            logger.error(
                f"Erro no bulk insert ({model.__tablename__}): {e}",
                exc_info=True
            )

            raise