# create_tables.py

import logging
import traceback

from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine
from app.models import Base
from app.config import Settings

logger = logging.getLogger(__name__)


# =====================================================
# MYSQL SESSION TUNING
# =====================================================
MYSQL_TUNING = [

    # Melhor performance ETL
    "SET FOREIGN_KEY_CHECKS = 0",

    # Timeout lock
    "SET SESSION innodb_lock_wait_timeout = 300",

    # Melhor insert massivo
    "SET SESSION sql_log_bin = 0",

    # Otimização temp table
    "SET SESSION bulk_insert_buffer_size = 268435456"
]


# =====================================================
# SHOW TABLES
# =====================================================
def show_tables():

    inspector = inspect(engine)

    tables = inspector.get_table_names()

    logger.info("=" * 101)
    logger.info("TABELAS ENCONTRADAS")
    logger.info("=" * 101)

    for table in sorted(tables):

        logger.info(f"✔ {table}")

    logger.info("=" * 101)


# =====================================================
# APPLY MYSQL TUNING
# =====================================================
def apply_mysql_tuning():

    logger.info("Aplicando tuning MySQL...")

    try:

        with engine.connect() as conn:

            for sql in MYSQL_TUNING:

                conn.execute(text(sql))

            conn.commit()

        logger.info("Tuning MySQL aplicado")

    except SQLAlchemyError as e:

        logger.error(
            f"Erro tuning MySQL: {e}",
            exc_info=True
        )

        raise


# =====================================================
# CREATE ORM TABLES
# =====================================================
def create_orm_tables():

    logger.info("Criando tabelas ORM...")

    try:

        Base.metadata.create_all(bind=engine)

        logger.info("Tabelas ORM criadas")

    except SQLAlchemyError as e:

        logger.error(
            f"Erro create_all: {e}",
            exc_info=True
        )

        raise


# =====================================================
# VALIDATE DATABASE CONNECTION
# =====================================================
def validate_connection():

    logger.info("Validando conexão banco...")

    try:

        with engine.connect() as conn:

            conn.execute(text("SELECT 1"))

        logger.info("Conexão OK")

    except Exception as e:

        logger.error(
            f"Falha conexão banco: {e}",
            exc_info=True
        )

        raise


# =====================================================
# CREATE REQUIRED DIRECTORIES
# =====================================================
def create_directories():

    logger.info("Criando diretórios...")

    Settings.create_dirs()

    logger.info("Diretórios OK")


# =====================================================
# CREATE TABLES
# =====================================================
def create_tables():

    logger.info("=" * 101)
    logger.info(f"{Settings.APP_NAME}")
    logger.info("CREATE TABLES ENTERPRISE")
    logger.info("=" * 101)

    try:

        # =============================================
        # DIRECTORIES
        # =============================================
        create_directories()

        # =============================================
        # CONNECTION
        # =============================================
        validate_connection()

        # =============================================
        # MYSQL TUNING
        # =============================================
        apply_mysql_tuning()

        # =============================================
        # CREATE ORM TABLES
        # =============================================
        create_orm_tables()

        # =============================================
        # SHOW TABLES
        # =============================================
        show_tables()

        logger.info("=" * 101)
        logger.info("ESTRUTURA CRIADA COM SUCESSO")
        logger.info("=" * 101)

    except Exception as e:

        logger.error("=" * 101)
        logger.error("ERRO CRÍTICO CREATE TABLES")
        logger.error("=" * 101)

        logger.error(str(e))

        logger.error(traceback.format_exc())

        raise


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    create_tables()