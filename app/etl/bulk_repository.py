from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert
import logging
import time

logger = logging.getLogger(__name__)


class BulkRepository:

    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # BULK INSERT ENTERPRISE
    # =====================================================
    def bulk_insert(self, model, data: list, retries=3):
        """
        Insert em lote robusto com:
        - IGNORE duplicados
        - Retry automático
        - Fallback em erro
        """

        if not data:
            logger.debug("Nenhum dado para inserir")
            return 0

        for attempt in range(1, retries + 1):

            try:
                stmt = insert(model).values(data)

                # 🔥 IGNORE duplicados
                stmt = stmt.prefix_with("IGNORE")

                result = self.db.execute(stmt)
                self.db.commit()

                inserted = result.rowcount

                logger.info(
                    f"{model.__tablename__}: "
                    f"{inserted}/{len(data)} inseridos (duplicates ignorados)"
                )

                return inserted

            except SQLAlchemyError as e:

                self.db.rollback()

                logger.warning(
                    f"Tentativa {attempt}/{retries} falhou: {e}"
                )

                # última tentativa → fallback
                if attempt == retries:
                    logger.error(
                        f"Falha definitiva no lote. Aplicando fallback..."
                    )

                    return self._fallback_insert(model, data)

                time.sleep(2)

    # =====================================================
    # FALLBACK (linha a linha)
    # =====================================================
    def _fallback_insert(self, model, data: list):
        """
        Quando o batch falha:
        tenta inserir linha por linha
        """

        success = 0

        for row in data:
            try:
                stmt = insert(model).values(**row)
                stmt = stmt.prefix_with("IGNORE")

                self.db.execute(stmt)
                success += 1

            except Exception as e:
                logger.error(f"Registro inválido ignorado: {row} | erro: {e}")

        self.db.commit()

        logger.warning(
            f"Fallback concluído: {success}/{len(data)} inseridos"
        )

        return success
    
    # =====================================================
    # TRUNCATE TABLE
    # =====================================================
    def truncate_table(self, table_name: str):

        logger.info(f"Limpando tabela: {table_name}")

        try:

            self.db.execute(
                text(f"TRUNCATE TABLE {table_name}")
            )

            self.db.commit()

            logger.info(
                f"{table_name} truncada com sucesso"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro truncate {table_name}: {e}",
                exc_info=True
            )

            raise    