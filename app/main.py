import time
import traceback
from pathlib import Path

from app.config import Settings
from app.logger import setup_logger
from app.database import Base, engine
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
    logger.info("Criando estrutura banco...")
    Base.metadata.create_all(bind=engine)


def run_etl(logger):
    """
    Executa pipeline principal
    """
    logger.info("Iniciando pipelines ETL...")

    orchestrator = ETLOrchestrator(logger)
    orchestrator.run()


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

    except Exception as e:
        logger.error("=" * 70)
        logger.error("ERRO FATAL NA EXECUÇÃO")
        logger.error(str(e))
        logger.error(traceback.format_exc())
        logger.error("=" * 70)


if __name__ == "__main__":
    main()