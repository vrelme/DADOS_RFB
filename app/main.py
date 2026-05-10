import time
import traceback
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.exceptions import AppError, DatabaseOperationError
from app.logger import setup_logger
from app.database import Base, engine, ensure_database_exists
from app.etl.orchestrator import ETLOrchestrator


def banner(logger):
    logger.info("=" * 70)
    logger.info(Settings.APP_NAME)
    logger.info("Inicialização ambiente de produção")
    logger.info("=" * 70)


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


def create_database(logger):
    """
    Cria tabelas ORM
    """
    logger.info(f"Banco alvo.......: {Settings.ACTIVE_DB_NAME}")
    logger.info(f"Estratégia sync..: {Settings.SYNC_STRATEGY}")
    logger.info(f"Destino carga....: {Settings.LOAD_TARGET}")
    logger.info("Criando banco se necessário...")
    ensure_database_exists()
    logger.info("Criando estrutura banco...")
    Base.metadata.create_all(bind=engine)


def run_etl(logger):
    """
    Executa pipeline principal
    """
    logger.info("Iniciando pipelines ETL...")

    orchestrator = ETLOrchestrator()
    orchestrator.run()


def mysql_error_code(error):
    original = getattr(error, "orig", None)
    args = getattr(original, "args", None)
    return args[0] if args else None


def main():
    logger = setup_logger()
    start = time.time()

    try:
        banner(logger)

        logger.info("Validando ambiente...")

        validate_directories(logger)

        has_files = validate_input_files(logger)

        create_database(logger)

        if not has_files:
            logger.warning("Execução encerrada: sem arquivos para processar.")
            return

        run_etl(logger)

        total = time.time() - start

        logger.info("=" * 70)
        logger.info(f"Processamento finalizado com sucesso em {total:.2f}s")
        logger.info("=" * 70)

    except DatabaseOperationError as e:
        logger.error("=" * 70)
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
        logger.error("=" * 70)

    except AppError as e:
        logger.error("=" * 70)
        logger.error("EXECUÇÃO INTERROMPIDA")
        logger.error(str(e))
        logger.error("=" * 70)

    except SQLAlchemyError as e:
        logger.error("=" * 70)
        logger.error("EXECUÇÃO INTERROMPIDA POR ERRO DE BANCO")
        if mysql_error_code(e) == 2003:
            logger.error("Não foi possível conectar ao MySQL/MariaDB.")
            logger.error("Verifique se o serviço está iniciado e se DB_HOST/DB_PORT estão corretos.")
        else:
            logger.error(str(e))
        logger.error(traceback.format_exc())
        logger.error("=" * 70)

    except Exception as e:
        logger.error("=" * 70)
        logger.error("ERRO FATAL NA EXECUÇÃO")
        logger.error(str(e))
        logger.error(traceback.format_exc())
        logger.error("=" * 70)


if __name__ == "__main__":
    main()
