import time
import traceback
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.exceptions import AppError, DatabaseOperationError
from app.logger import setup_logger
from app.database import (
    Base,
    engine,
    operational_engine,
    ensure_database_exists,
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


RAW_IMPORT_FAST_TABLE_SQL = {
    "empresa": """
        CREATE TABLE IF NOT EXISTS empresa (
            cnpj_basico VARCHAR(8) NULL,
            razao_social VARCHAR(255) NULL,
            natureza_juridica INT NULL,
            qualificacao_responsavel INT NULL,
            capital_social DECIMAL(18, 2) NULL,
            porte_empresa INT NULL,
            ente_federativo VARCHAR(255) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "estabelecimento": """
        CREATE TABLE IF NOT EXISTS estabelecimento (
            cnpj_basico VARCHAR(8) NULL,
            cnpj_ordem VARCHAR(4) NULL,
            cnpj_dv VARCHAR(2) NULL,
            identificador_matriz_filial VARCHAR(1) NULL,
            nome_fantasia VARCHAR(255) NULL,
            situacao_cadastral VARCHAR(2) NULL,
            data_situacao_cadastral VARCHAR(8) NULL,
            motivo_situacao_cadastral VARCHAR(2) NULL,
            nome_cidade_exterior VARCHAR(255) NULL,
            pais VARCHAR(3) NULL,
            data_inicio_atividade VARCHAR(8) NULL,
            cnae_fiscal_principal VARCHAR(7) NULL,
            cnae_fiscal_secundaria TEXT NULL,
            tipo_logradouro VARCHAR(50) NULL,
            logradouro VARCHAR(255) NULL,
            numero VARCHAR(20) NULL,
            complemento VARCHAR(255) NULL,
            bairro VARCHAR(100) NULL,
            cep VARCHAR(8) NULL,
            uf VARCHAR(2) NULL,
            municipio VARCHAR(4) NULL,
            ddd1 VARCHAR(4) NULL,
            telefone1 VARCHAR(20) NULL,
            ddd2 VARCHAR(4) NULL,
            telefone2 VARCHAR(20) NULL,
            ddd_fax VARCHAR(4) NULL,
            fax VARCHAR(20) NULL,
            email VARCHAR(255) NULL,
            situacao_especial VARCHAR(255) NULL,
            data_situacao_especial VARCHAR(8) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "socio": """
        CREATE TABLE IF NOT EXISTS socio (
            cnpj_basico VARCHAR(8) NULL,
            identificador_socio VARCHAR(1) NULL,
            nome_socio VARCHAR(255) NULL,
            cpf_cnpj_socio VARCHAR(14) NULL,
            qualificacao_socio VARCHAR(2) NULL,
            data_entrada_sociedade VARCHAR(8) NULL,
            pais VARCHAR(3) NULL,
            representante_legal VARCHAR(11) NULL,
            nome_representante VARCHAR(255) NULL,
            qualificacao_representante_legal VARCHAR(2) NULL,
            faixa_etaria VARCHAR(1) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
}


def create_fast_raw_import_tables(logger):
    with engine.begin() as conn:
        if Settings.RAW_IMPORT_RESET_TABLES:
            for table_name in ["socio", "estabelecimento", "empresa"]:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        for table_name, ddl in RAW_IMPORT_FAST_TABLE_SQL.items():
            conn.execute(text(ddl))
            logger.info(f"Tabela raw import pronta: {table_name}")


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
