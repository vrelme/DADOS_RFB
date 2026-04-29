from app.logger import setup_logger
from app.utils import create_folders
from app.config import Settings

def main():
    logger = setup_logger()
    create_folders()

    logger.info("=" * 60)
    logger.info(Settings.APP_NAME)
    logger.info("Inicializando arquitetura V4...")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()