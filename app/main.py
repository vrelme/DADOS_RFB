import time
import traceback
import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.exceptions import AppError, DatabaseOperationError
from app.logger import setup_logger
from app.database import (
    Base,
    create_server_engine,
    engine,
    operational_engine,
    ensure_database_exists,
    is_database_connection_lost,
    mysql_error_code,
    wait_for_database_recovery,
)
from app.models import (
    Empresa,
    Estabelecimento,
    Socio,
    ETLExecution,
    ETLRun,
    ETLRunPhase,
    ETLFileProgress,
    ETLMetric,
    ETLCheckpoint,
    ETLDeadLetter,
    DataQualityRule,
)
from app.etl.orchestrator import ETLOrchestrator
from app.etl.rfb_manifest import RFB_TABLES, raw_import_create_table_sql


error_detail_logger = logging.getLogger("error_detail")


def banner(logger):
    logger.info("=" * 107)
    logger.info(Settings.APP_NAME)
    logger.info(f"Versão da aplicação: {Settings.APP_VERSION}")
    logger.info("Inicialização ambiente de produção")
    logger.info("=" * 107)


def validate_directories(logger):
    """
    Garante estrutura de pastas
    """
    logger.info("Validando diretórios...")

    dirs = [
        Settings.INPUT_DIR,
        Settings.PROCESSED_DIR,
        Settings.ERROR_DIR,
        Settings.LOG_DIR,
    ]

    for folder in dirs:
        Path(folder).mkdir(parents=True, exist_ok=True)

    logger.info(f"INPUT_DIR......: {Settings.INPUT_DIR}")
    logger.info(f"PROCESSED_DIR..: {Settings.PROCESSED_DIR}")
    logger.info(f"ERROR_DIR......: {Settings.ERROR_DIR}")
    logger.info(f"LOG_DIR........: {Settings.LOG_DIR}")


def validate_input_files(logger):
    """
    Lista arquivos disponíveis
    """
    logger.info("Verificando arquivos de entrada...")

    files = list(Path(Settings.INPUT_DIR).glob("*"))

    if not files:
        logger.warning("Nenhum arquivo encontrado na pasta de entrada.")
        return False

    logger.info(f"Total arquivos encontrados: {len(files)}")

    for f in files[:15]:
        logger.info(f" - {f.name}")

    if len(files) > 15:
        logger.info("...")

    return True


def raw_import_tables():
    return [
        Empresa.__table__,
        Estabelecimento.__table__,
        Socio.__table__,
    ]


def operational_tables():
    return [
        ETLExecution.__table__,
        ETLRun.__table__,
        ETLRunPhase.__table__,
        ETLFileProgress.__table__,
        ETLMetric.__table__,
        ETLCheckpoint.__table__,
        ETLDeadLetter.__table__,
        DataQualityRule.__table__,
    ]


def create_table_set(logger, bind, tables, reset=False):
    if reset:
        Base.metadata.drop_all(bind=bind, tables=list(reversed(tables)))

    Base.metadata.create_all(bind=bind, tables=tables)
    table_names = ", ".join(table.name for table in tables)
    logger.info(f"Tabelas criadas/verificadas: {table_names}")


def validate_raw_import_reset_target():
    protected_databases = {
        Settings.DB_NAME.lower(),
        Settings.OPERATIONAL_DB_NAME.lower(),
    }
    active_database = Settings.ACTIVE_DB_NAME.lower()

    if active_database in protected_databases:
        raise RuntimeError(
            "RAW_IMPORT_RESET_SCHEMA bloquearia um banco protegido. "
            f"ACTIVE_DB_NAME={Settings.ACTIVE_DB_NAME}; "
            f"DB_NAME={Settings.DB_NAME}; "
            f"OPERATIONAL_DB_NAME={Settings.OPERATIONAL_DB_NAME}"
        )


def reset_raw_import_schema(logger):
    if not (
        Settings.SYNC_STRATEGY in Settings.RAW_IMPORT_STRATEGIES
        and Settings.RAW_IMPORT_RESET_SCHEMA
    ):
        return

    validate_raw_import_reset_target()
    logger.info(
        f"RAW_IMPORT_RESET_SCHEMA=True | DROP DATABASE {Settings.ACTIVE_DB_NAME}"
    )
    ensure_database_exists(Settings.ACTIVE_DB_NAME)
    safe_name = Settings.ACTIVE_DB_NAME.replace("`", "``")
    admin_engine = create_server_engine(
        read_timeout=Settings.DB_PROMOTION_READ_TIMEOUT,
        write_timeout=Settings.DB_PROMOTION_WRITE_TIMEOUT,
    )
    try:
        with admin_engine.begin() as conn:
            conn.execute(
                text(
                    "SET SESSION lock_wait_timeout = "
                    f"{int(Settings.DB_PROMOTION_LOCK_WAIT_TIMEOUT)}"
                )
            )
            conn.execute(text(f"DROP DATABASE IF EXISTS `{safe_name}`"))
    finally:
        admin_engine.dispose()

    engine.dispose()

    logger.info(
        f"RAW_IMPORT_RESET_SCHEMA=True | CREATE DATABASE {Settings.ACTIVE_DB_NAME}"
    )
    ensure_database_exists(Settings.ACTIVE_DB_NAME)
    engine.dispose()


def create_fast_raw_import_tables(logger):
    safe_name = Settings.ACTIVE_DB_NAME.replace("`", "``")

    with engine.begin() as conn:
        conn.execute(text(f"USE `{safe_name}`"))

        if Settings.RAW_IMPORT_RESET_TABLES and not Settings.RAW_IMPORT_RESET_SCHEMA:
            for table in reversed(RFB_TABLES):
                conn.execute(text(f"DROP TABLE IF EXISTS {table.table_name}"))

        for table in RFB_TABLES:
            conn.execute(text(raw_import_create_table_sql(table)))
            logger.info(f"Tabela raw import pronta: {table.table_name}")


def create_database(logger):
    """
    Cria bancos e tabelas conforme a função de cada schema.
    """
    logger.info(f"Banco carga......: {Settings.ACTIVE_DB_NAME}")
    logger.info(f"Banco operacional: {Settings.OPERATIONAL_DB_NAME}")
    logger.info(f"Estratégia sync..: {Settings.SYNC_STRATEGY}")
    logger.info(f"Destino carga....: {Settings.LOAD_TARGET}")
    logger.info("Criando bancos se necessário...")

    ensure_database_exists(Settings.ACTIVE_DB_NAME)
    ensure_database_exists(Settings.OPERATIONAL_DB_NAME)
    reset_raw_import_schema(logger)

    logger.info("Criando estrutura banco de carga...")
    if Settings.SYNC_STRATEGY in Settings.RAW_IMPORT_STRATEGIES:
        if Settings.RAW_IMPORT_FAST_SCHEMA:
            create_fast_raw_import_tables(logger)
        else:
            create_table_set(
                logger,
                engine,
                raw_import_tables(),
                reset=Settings.RAW_IMPORT_RESET_TABLES,
            )
    else:
        Base.metadata.create_all(bind=engine)
        logger.info("Tabelas de dados criadas/verificadas no banco principal")

    logger.info("Criando estrutura banco operacional...")
    create_table_set(logger, operational_engine, operational_tables())


def run_etl(logger):
    """
    Executa pipeline principal
    """
    logger.info("Iniciando pipelines ETL...")

    orchestrator = ETLOrchestrator(logger_instance=logger)
    orchestrator.run()


def execute_with_database_recovery(logger, operation_name, operation):
    attempt = 0

    while True:
        attempt += 1
        try:
            return operation()

        except (SQLAlchemyError, DatabaseOperationError) as exc:
            if not (
                Settings.DB_RECOVERY_ENABLED
                and is_database_connection_lost(exc)
            ):
                raise

            logger.error(
                f"DB RECOVERY | conexao perdida | "
                f"operacao={operation_name} | tentativa_execucao={attempt} | "
                "detalhes gravados em error.log"
            )
            error_detail_logger.error(
                f"DB RECOVERY | conexao perdida | "
                f"operacao={operation_name} | tentativa_execucao={attempt} | "
                f"erro={exc}",
                exc_info=True,
            )
            wait_for_database_recovery(
                logger,
                context=f"operacao={operation_name}",
            )
            logger.info(
                f"DB RECOVERY | retomando operacao | "
                f"operacao={operation_name} | proxima_tentativa={attempt + 1}"
            )


def main():
    logger = setup_logger()
    start = time.time()

    try:
        banner(logger)

        logger.info("Validando ambiente...")

        validate_directories(logger)

        has_files = validate_input_files(logger)

        execute_with_database_recovery(
            logger,
            "create_database",
            lambda: create_database(logger),
        )

        if not has_files:
            logger.warning("Execução encerrada: sem arquivos para processar.")
            return

        etl_orchestrator = ETLOrchestrator(logger_instance=logger)
        execute_with_database_recovery(
            logger,
            "run_etl",
            etl_orchestrator.run,
        )

        total = time.time() - start

        logger.info("=" * 107)
        logger.info(f"Processamento finalizado com sucesso em {total:.2f}s")
        logger.info("=" * 107)

    except DatabaseOperationError as e:
        logger.error("=" * 107)
        logger.error("EXECUÇÃO INTERROMPIDA POR ERRO DE BANCO")
        logger.error(f"Operação: {e.operation}")
        logger.error(f"Tabela: {e.table_name}")
        logger.error(e.user_message)
        logger.error("Ação recomendada:")
        logger.error("1. Pare outras execuções do ETL.")
        logger.error("2. Feche consultas/transações abertas no Workbench.")
        logger.error("3. Rode SHOW FULL PROCESSLIST e finalize sessões bloqueadoras.")
        logger.error("4. Para carga completa, use MERGE_STRATEGY=full_refresh.")
        logger.error(f"Erro original: {e.original_error}")
        logger.error("=" * 107)
        error_detail_logger.error("DatabaseOperationError fatal", exc_info=True)

    except AppError as e:
        logger.error("=" * 107)
        logger.error("EXECUÇÃO INTERROMPIDA")
        logger.error(str(e))
        logger.error("=" * 107)
        error_detail_logger.error("AppError fatal", exc_info=True)

    except SQLAlchemyError as e:
        logger.error("=" * 107)
        logger.error("EXECUÇÃO INTERROMPIDA POR ERRO DE BANCO")
        code = mysql_error_code(e)
        if code == 2003:
            logger.error("Não foi possível conectar ao MySQL/MariaDB.")
            logger.error("Verifique se o serviço está iniciado e se DB_HOST/DB_PORT estão corretos.")
        elif code in {2006, 2013}:
            logger.error("A conexão com o MySQL/MariaDB caiu durante a operação.")
            logger.error("Verifique se o serviço reiniciou, se há limite de timeout/conexões ou queda no host.")
            logger.error("A aplicação tentará retry nas operações administrativas configuradas.")
        else:
            logger.error(str(e))
        logger.error("Detalhes tecnicos gravados em error.log")
        error_detail_logger.error(f"SQLAlchemyError fatal: {e}", exc_info=True)
        logger.error("=" * 107)

    except Exception as e:
        logger.error("=" * 107)
        logger.error("ERRO FATAL NA EXECUÇÃO")
        logger.error(str(e))
        logger.error("Detalhes tecnicos gravados em error.log")
        error_detail_logger.error(f"Erro fatal nao tratado: {e}", exc_info=True)
        logger.error("=" * 107)


if __name__ == "__main__":
    main()
