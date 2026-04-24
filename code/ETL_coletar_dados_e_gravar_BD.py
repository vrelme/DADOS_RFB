"""
Script para processamento de dados públicos do CNPJ da Receita Federal do Brasil.

Desenvolvido por: Aphonso Henrique do Amaral Rafael
Adaptado por: Vander Ribeiro Elme

"""

import os
import time
import logging
import zipfile
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import urljoin
from pathlib import Path
import contextlib
import mysql.connector
from mysql.connector import errors as mysql_errors
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import shutil


class Config:
    """Configurações da aplicação."""
    
    # Configuração do logging
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOG_FILE =  'DADOS_RFB.log'

    # DIR base
    OUTPUT_DIR = None
    EXTRACT_DIR = None


    # Tamanhos de lote
    BATCH_SIZE = 10000
    MAX_RETRIES = 3
    RETRY_DELAY = 5

    # Controla se as tabelas devem ser sempre recriadas (drop/create)
    DROP_AND_RECREATE_TABLES = True  # Altere para False para manter dados existentes

class DatabaseManager:
    """Gerenciador de conexões com o banco de dados."""
    
    def __init__(self):
        self.connection = None

    def connect(self)-> mysql.connector.MySQLConnection:
        """Conecta ao banco de dados com tentativas de reconexão."""
        for attempt in range(Config.MAX_RETRIES):
            try:
                self.connection = mysql.connector.connect(
                    host=os.getenv('DB_HOST'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    database=os.getenv('DB_NAME'),
                    # use_pure=True, 
                    auth_plugin='mysql_native_password',
                    charset='utf8mb4',
                    collation='utf8mb4_unicode_ci',
                    raise_on_warnings=True,
                    connection_timeout=30
                )

                logging.info("Conexão com o banco de dados estabelecida com sucesso")
                logging.info("Versão do banco: %s", self.connection.get_server_info())
                return self.connection
        
            except mysql_errors.Error as e:
                logging.error("Erro ao conectar ao banco de dados (tentativa %d): %s", attempt + 1, e)
                if attempt < Config.MAX_RETRIES - 1:
                    logging.info("Tentando reconectar em %d segundos...", Config.RETRY_DELAY)
                    time.sleep(Config.RETRY_DELAY)
                else:
                    raise

    def ensure_connection(self):
            """Garante que a conexão com o banco de dados esteja ativa."""
            if not self.connection or not self.connection.is_connected():
                self.connect()

    @contextlib.contextmanager
    def get_cursor(self):
        """Context manager para gerenciar cursor de banco de dados."""
        cursor = None
        try:
            self.ensure_connection()
            cursor = self.connection.cursor()
            yield cursor
        except mysql_errors.Error as e:
            if cursor and self.connection:
                self.connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()


class FileProcessor:
    """Processador de arquivos."""

    @staticmethod
    def create_directories(*paths: str) -> List[Path]:
        """Cria diretórios recursivamente."""
        created_paths = []
        for path in paths:
            path_obj = Path(path)
            try:
                path_obj.mkdir(parents=True, exist_ok=True, mode=0o755)
                created_paths.append(path_obj)
                logging.info("Diretório criado: %s", path_obj)
            except OSError as e:
                logging.error("Erro ao criar diretório %s: %s", path_obj, e)
                raise
        return created_paths
    
       
    @staticmethod
    def download_progress(current: int, total: int, width: int = 80):
        """Barra de progresso para download."""
        porcentagem = (current * 100) / total
        mensagem = f"Download: {porcentagem:.1f}% [{current} / {total}] bytes"
        print(f"\r{mensagem}", end="", flush=True)

    @staticmethod
    def extract_files(zip_files: List[Path], extract_to: Path):
        """Descompacta arquivos ZIP."""
        for zip_file in zip_files:
            try:
                logging.info("Descompactando: %s", zip_file)
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
            except (zipfile.BadZipFile, OSError) as e:
                logging.error ("Erro ao descompactar %s: %s", zip_file, e)
                raise        

    def reset_extract_dir(self):
        if Config.EXTRACT_DIR.exists():
            shutil.rmtree(Config.EXTRACT_DIR)

        Config.EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
        logging.info("Diretório de extração resetado")           

class DataProcessor:
    """"Processador de dados."""

    @staticmethod
    def categorize_files(file_list: List[str]) -> Dict[str, List[str]]:
        """"Categoriza arquivos por tipo."""
        categorias = {
            'empresa': 'EMPRE',
            'estabelecimento': 'ESTABELE',
            'socios': 'SOCIO',
            'simples': 'SIMPLES',
            'cnae': 'CNAE',
            'moti': 'MOTI',
            'munic': 'MUNIC',
            'natju': 'NATJU',
            'pais': 'PAIS',
            'quals': 'QUALS'
        }

        categorizado = {categoria:[] for categoria in categorias}

        for filename in file_list:
            for categoria, padrao in categorias.items():
                if padrao in filename.upper():
                    categorizado[categoria].append(filename)
                    break
        return categorizado
    
    @staticmethod
    def convert_to_native_types(value: Any) -> Any:
        """Converte tipos pandas/numpy para tipos nativos Python."""
        if value is None or pd.isna(value):
            return None
        elif isinstance(value, (pd.Timestamp, np.datetime64)):
            try:
                return pd.Timestamp(value).to_pydatetime().date()
            except Exception:
                return None
        elif isinstance(value, (np.integer, np.int32, np.int64)):
            return int(value)
        elif isinstance(value, (np.floating, np.float64)):
            return float(value) if not pd.isna(value) else None
        elif isinstance(value, (np.bool_, bool)):
            return bool(value)
        elif isinstance(value, (str, bytes)):
            return str(value).strip() if value else None
        elif isinstance(value, np.generic):
            return value.item()
        else:
            return value    
        
class RFBDataLoader:
    """Carregador principal de dados da RFB."""
    def __init__(self, env_path:str):
        self.env_path = Path(env_path)
        self.db_manager = DatabaseManager()
        self.file_processor = FileProcessor()
        self.data_processor = DataProcessor()

        self.setup_logging()
        self.load_environment()

        self.output_dir, self.extract_dir = self.setup_directories()
        self.files = []

    def setup_logging(self):
        """Configura o sistema de logging."""
        logging.basicConfig(
            filename = Config.LOG_FILE,
            level = logging.INFO,
            format = Config.LOG_FORMAT
        )

    def load_environment(self):
        """Carrega variáveis de ambiente."""
        dotenv_path = self.env_path / '.env'
        if dotenv_path.exists():
            load_dotenv(dotenv_path = dotenv_path)
            Config.OUTPUT_DIR = Path(os.getenv('OUTPUT_DIR'))
            Config.EXTRACT_DIR = Path(os.getenv('EXTRACT_DIR'))

            logging.info("Variáveis de ambiente carregadas de: %s", dotenv_path)
        else:
            logging.warning("Ärquivo .env não encontrado em: %s", dotenv_path)

    def setup_directories(self):
        """Configura dirétorios de trabalho."""

        self.file_processor.create_directories(Config.OUTPUT_DIR, Config.EXTRACT_DIR)
        return Config.OUTPUT_DIR, Config.EXTRACT_DIR

    def get_local_zip_files(self) -> List[Path]:
        """Lista arquivos ZIP do diretório local"""

        files = [
            f for f in Config.OUTPUT_DIR.iterdir()
            if f.is_file() and f.suffix.lower() == '.zip'
        ]

        if not files:
            logging.warning("Nenhum arquivo ZIP encontrado em %s", Config.OUTPUT_DIR)
        else:
            logging.info("Encontrados %d arquivos ZIP", len(files))

        return files

    def process_data_files(self, categorizado_files: Dict[str, List[str]]):
        """Pocessa os arquivos de dados e insere no banco de dados."""
        # Usar uma única conexão para todo processo
        conexao = None

        try: 
            conexao = self.db_manager.connect()
            # Garantir que o banco de dados existe e que estamos usando-o
            db_name = os.getenv('DB_NAME')
            if not db_name:
                raise RuntimeError("DB_NAME não definido nas variáveis de ambiente.")
            
            with self.db_manager.get_cursor() as cursor:
                # Apenas usar o banco, sem tentar criar
                cursor.execute(f"USE `{db_name}`")
                conexao.commit()

            table_definitions = self.get_table_definitions()

            # Ordem de processamento: tabelas de referência primeiro, depois dados principais
            processing_order = ['pais', 'munic', 'quals', 'natju', 'cnae', 'empresa', 'estabelecimento', 'socios', 'simples']

            for table_name in processing_order:
                if table_name in categorizado_files and categorizado_files[table_name]:
                    self.process_table_data(conexao, table_name, categorizado_files[table_name], table_definitions[table_name])
            
        except Exception as e:
            logging.error("Erro no processamento geral: %s", e)
            raise
        finally:
            # FECHAR a conexão ao final
            if conexao and conexao.is_connected():
                conexao.close()
                logging.info("Conexão com o banco fechada")

    def get_table_definitions(self) -> Dict[str, Dict]:
        """Retorna definições das tabelas."""
        return {
            'empresa': {
                'schema': """CREATE TABLE empresa (
                    cnpj_basico VARCHAR(14) PRIMARY KEY,
                    razao_social VARCHAR(255),
                    natureza_juridica INT,
                    qualificacao_responsavel INT,
                    capital_social DECIMAL(15,2),
                    porte_empresa INT,
                    ente_federativo_responsavel VARCHAR(255),
                    KEY idx_empresa_cnpj (cnpj_basico)
                )""",
                'columns':['cnpj_basico', 'razao_social', 'natureza_juridica',
                           'qualificacao_responsavel', 'capital_social', 'porte_empresa',
                           'ente_federativo_responsavel']
            },
            'estabelecimento': {
                'schema': """CREATE TABLE estabelecimento (
                    cnpj_basico VARCHAR(14) PRIMARY KEY,
                    cnpj_ordem VARCHAR(4),
                    cnpj_dv VARCHAR(2),
                    identificador_matriz_filial INT,
                    nome_fantasia VARCHAR(255),
                    situacao_cadastral INT,
                    data_situacao_cadastral DATE,
                    motivo_situacao_cadastral varchar(255),
                    nome_cidade_exterior VARCHAR(255),
                    pais VARCHAR(255),
                    data_inicio_atividade DATE,
                    cnae_fiscal_principal INT,
                    cnae_fiscal_secundaria VARCHAR(1000),
                    tipo_logradouro VARCHAR(255),
                    logradouro VARCHAR(255),
                    numero VARCHAR(10),
                    complemento VARCHAR(255),
                    bairro VARCHAR(255),
                    cep VARCHAR(8),
                    uf VARCHAR(2),
                    municipio INT,
                    ddd_1 VARCHAR(4),
                    telefone_1 VARCHAR(20),
                    ddd_2 VARCHAR(4),
                    telefone_2 VARCHAR(20),
                    dd_fax VARCHAR(4),
                    fax VARCHAR(20),
                    correio_eletronico VARCHAR(255),
                    situacao_especial VARCHAR(255),
                    data_situacao_especial DATE,
                    KEY idx_estabelecimento_cnpj (cnpj_basico)
                )""",
                'columns':['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
                           'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
                           'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                           'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
                           'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'dd_fax', 'fax', 'correio_eletronico',
                           'situacao_especial', 'data_situacao_especial']
            },
            'simples': {
                'schema': """CREATE TABLE simples (
                    cnpj_basico VARCHAR(14) PRIMARY KEY,
                    opcao_simples VARCHAR(1),
                    data_opcao_simples DATE,
                    data_exclusao_simples DATE,
                    opcao_mei VARCHAR(3),
                    data_opcao_mei DATE,
                    data_exclusao_mei DATE,
                    KEY idx_simples_cnpj (cnpj_basico) 
                )""",
                'columns': ['cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
            },
            'socios': {
                    'schema': """CREATE TABLE socios (
                        cnpj_basico VARCHAR(14) PRIMARY KEY,
                        identificador_socio INT,
                        nome_socio_razao_social VARCHAR(255),
                        cpf_cnpj_socio VARCHAR(14),
                        qualificacao_socio INT,
                        data_entrada_sociedade DATE,
                        pais INT,
                        representante_legal VARCHAR(255),
                        nome_do_representante VARCHAR(255),
                        qualificacao_representante_legal INT,
                        faixa_etaria INT,
                        KEY idx_socios_cnpj (cnpj_basico)
                    )""",
                    'columns': ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada_sociedade', 
                                'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']
            },
            'pais': {
                'schema': """CREATE TABLE pais (
                    codigo INT PRIMARY KEY,
                    nome VARCHAR(255)
                )""",
                'columns': ['codigo', 'nome']
            },
            'munic': {
                'schema': """CREATE TABLE munic (
                    codigo INT PRIMARY KEY,
                    nome VARCHAR(255)
                )""",
                'columns': ['codigo', 'nome']
            },
            'quals': {
                'schema': """CREATE TABLE quals (
                    codigo INT PRIMARY KEY,
                    nome VARCHAR(255)
                )""",
                'columns': ['codigo', 'nome']
            },
            'natju': {
                'schema': """CREATE TABLE natju (
                    codigo INT PRIMARY KEY,
                    nome VARCHAR(255)
                )""",
                'columns': ['codigo', 'nome']
            },
            'cnae': {
                'schema': """CREATE TABLE cnae (
                    codigo INT PRIMARY KEY,
                    nome VARCHAR(255)
                )""",
                'columns': ['codigo', 'nome']
            }
        }

    def process_table_data(self,conexao: mysql.connector.MySQLConnection,
                           table_name:str, file_list: List[str],
                           table_definition: Dict):
        """Processa dados de uma tabela específica."""
        cursor = None

        try:
            cursor = conexao.cursor()
            # Verificar se a tabela existe antes (apenas para logging)
            tabela_existia = self.verificar_tabela_existe(conexao, table_name)
            if tabela_existia:
                if Config.DROP_AND_RECREATE_TABLES:
                    logging.info(f"Tabela {table_name} existe e será recriada (drop + create).")
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")    # ← DROP se existir
                        cursor.execute(table_definition['schema'])              # ← CREATE TABLE
                        conexao.commit()
                        logging.info(f"Tabela {table_name} recriada com sucesso.")
                    except mysql_errors.Error as e:
                        conexao.rollback()
                        logging.error(f"Erro ao recriar tabela {table_name}: {e}")
                        logging.error(f"SQL executado: {table_definition['schema']}")  
                        raise
                else:
                    logging.info(f"Tabela {table_name} existe e será mantida (DROP desativado).")  
            
            else:
                # tabela não existia -> criar
                logging.info(f"Tabela {table_name} não existe. Criando tabela...")
                try:
                    cursor.execute(table_definition['schema'])              # ← CREATE TABLE
                    conexao.commit()
                    logging.info(f"Tabela {table_name} criada com sucesso.")
                except mysql_errors.Error as e:
                    conexao.rollback()
                    logging.error(f"Erro ao criar tabela {table_name}: {e}")
                    logging.error(f"SQL executado: {table_definition['schema']}")
                    raise

            # Processar cada arquivo
            for filename in file_list:
                file_path = self.extract_dir / filename
                if not file_path.exists():
                    logging.warning("Arquivo não encontrado: %s", file_path)
                    continue
                    
                logging.info("Processando arquivo: %s", filename)

                self.process_single_file_optimized(conexao, table_name, file_path, 
                                                  table_definition['columns'])
        except Exception as e:
            logging.error("Erro ao processar tabela %s: %s", table_name, e)
            raise
        finally:
            if cursor:
                cursor.close()
                
    def verificar_tabela_existe(self, conexao: mysql.connector.MySQLConnection, table_name: str) -> bool:
        """Verifica se uma tabela existe no banco de dados."""
        cursor = None
        try:
            db_name = os.getenv('DB_NAME')
            if not db_name:
                logging.warning("DB_NAME nào definido ao verificar existência de tabela")
                db_cond = "DATABASE()"
            else:
                db_cond = f"'{db_name}'"

            cursor = conexao.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = {db_cond} 
                AND table_name = %s
            """, (table_name,))
            resultado = cursor.fetchone()
            return bool(resultado and resultado[0] > 0)
        
        except mysql_errors.Error as e:
            logging.error(f"Erro ao verificar existência da tabela {table_name}: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def process_single_file_optimized(self, conexao: mysql.connector.MySQLConnection,
                                 table_name: str, file_path: Path,
                                 column_names: List[str]):
        try:
            # Configurações otimizadas para leitura CSV
            dtype_spec = {col: 'string' for col in column_names}  # Ler tudo como string primeiro
        
            # Ler arquivo em chunks para melhor performance
            chunk_size = 50000
            total_rows = 0
            chunk_number = 0

            for chunk in pd.read_csv(
                file_path,
                sep =';', 
                encoding='latin1',
                names=column_names,
                header=None,
                dtype=dtype_spec,
                chunksize=chunk_size, 
                low_memory=False,
                na_filter=True,
                keep_default_na=False,
                na_values=['', 'NULL', 'null'],
                
            ):
                chunk_number += 1

                chunk.columns = column_names

                # Aplicar transformações otimizadas
                chunk = self.sanitize_chunk(chunk, column_names)

                data = list(chunk.itertuples(index=False, name=None))

                # Inserir dados no banco
                if data:
                    self.batch_insert_data_optimized(conexao, table_name, data, column_names)
                    total_rows += len(data)

                logging.info("Processadas %d linhas da tabela %s (chunk %d)", 
                             total_rows, table_name, chunk_number)
            
            logging.info("Total de %d linhas processadas para tabela %s", total_rows, table_name)

        except Exception as e:
            logging.error("Erro ao processar arquivo %s: %s", file_path, e)
            raise
   
    def get_date_columns(self, column_names: List[str]) -> List[str]:
        """Identifica colunas de data para parsing automático."""
        date_columns = []
        for col in column_names:
            if any(keyword in col for keyword in ['data', 'date']):
                date_columns.append(col)
        return date_columns


    def sanitize_chunk(self, df: pd.DataFrame, column_names: list) -> pd.DataFrame:

        def clean_string(s):
            s = s.astype(str).str.strip()
            s = s.str.replace('"', '', regex=False)
            s = s.str.replace('\xa0', '', regex=False)
            s = s.replace({
                '': None,
                'nan': None,
                'NaN': None,
                'None': None,
                'NULL': None,
                'null': None
            })
            return s

        def clean_int(s):
            s = clean_string(s)
            s = s.fillna('').astype(str)
            s = s.str.replace(r'[^0-9\-]', '', regex=True)
            s = s.replace({'': None})
            s = pd.to_numeric(s, errors='coerce')
            return s.apply(lambda x: int(x) if pd.notnull(x) else None)

        def clean_float(s):
            s = clean_string(s)
            s = s.fillna('').astype(str)
            s = s.str.replace('.', '', regex=False)
            s = s.str.replace(',', '.', regex=False)
            s = s.replace({'': None})
            s = pd.to_numeric(s, errors='coerce')
            return s.apply(lambda x: float(x) if pd.notnull(x) else None)

        def clean_date(s):
            s = clean_string(s)
            s = s.fillna('').astype(str)
            s = s.str.replace(r'[^0-9]', '', regex=True)
            s = s.replace({'': None})
            s = pd.to_datetime(s, format='%Y%m%d', errors='coerce')
            return s.dt.date

        for col in column_names:
            if col not in df.columns:
                continue

            try:
                if col in [
                    'natureza_juridica', 'qualificacao_responsavel',
                    'identificador_matriz_filial', 'situacao_cadastral',
                    'porte_empresa', 'municipio', 'pais',
                    'faixa_etaria', 'codigo'
                ]:
                    df[col] = clean_int(df[col])

                elif col == 'capital_social':
                    df[col] = clean_float(df[col])

                elif 'data' in col.lower():
                    df[col] = clean_date(df[col])

                else:
                    df[col] = clean_string(df[col])

            except Exception as e:
                logging.warning(f"Erro ao sanitizar coluna {col}: {e}")

        # GARANTIA FINAL (CRÍTICO)
        df = df.replace({np.nan: None})

        return df

    def batch_insert_data_optimized(self, conexao: mysql.connector.MySQLConnection,
                                   table_name: str, data: List[Tuple],
                                   column_names: List[str], attempt: int =1):
        """Insere dados em lotes com otimizações."""
        if not data:
            return
        
        cursor = None
        max_attempts = 3 # Número máximo de tentativas
        try:
            # Tentativas iterativas, reduzindo o tamanho do lote a cada tentativa
            for current_attempt in range(1, max_attempts + 1):
                try:
                    cursor = conexao.cursor()
                    columns = ', '.join(column_names)
                    placeholders = ', '.join(['%s'] * len(column_names))
                    insert_query = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
            
                    # calcula tamanho do lote para esta tentativa (mínimo 1)
                    current_batch_size = max(1, Config.BATCH_SIZE // (2 ** (current_attempt - 1)))

                    batch_count = 0
                    # tenta inserir todo o conjunto em batches de current_batch_size
                    for i in range(0, len(data), current_batch_size):
                        batch = data[i:i + current_batch_size]
                        cursor.executemany(insert_query, batch)
                        batch_count += 1
                        
                        if batch_count % 5 == 0:
                            conexao.commit()

                        logging.info(f"Inserido lote de {len(batch)} registros em {table_name} (total: {batch_count})")

                    # se chegou até aqui sem exceção, sucesso -> sair do loop de tentativa
                    conexao.commit()
                    return

                except mysql_errors.Error as e:
                    # rollback da tentativa atual
                    if conexao:
                        conexao.rollback()
                    logging.warning(f"Erro na tentativa {current_attempt} ao inserir dados em {table_name}: {e}")

                    # fechar cursor da tentativa antes de nova tentativa
                    if cursor:
                        cursor.close()
                        cursor = None

                    # se atingiu o máximo de tentativas, re-levanta
                    if current_attempt >= max_attempts:
                        logging.error(f"Falha ao inserir dados em {table_name} após {max_attempts} tentativas: {e}")
                        raise

                    # caso contrario, aguarda e tenta novamente com lote menor
                    logging.info("Tentando novamente com lote menor (tentativa %d)...", current_attempt + 1)
                    time.sleep(Config.RETRY_DELAY)
                   
        finally:
            if cursor:
                cursor.close()


    def create_indexes(self, conexao:mysql.connector.MySQLConnection):
        """Cria índices no banco de dados."""
        # Índices adcionais para consultas comuns
        cursor = None
        try:
            cursor = conexao.cursor()

            additional_indexes = [
            'CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON estabelecimento(uf)',
            'CREATE INDEX IF NOT EXISTS idx_establecimento_municipio ON estabelecimento(municipio)',
            'CREATE INDEX IF NOT EXISTS idx_establecimento_cnae ON estabelecimento(cnae_fiscal_principal)',
            'CREATE INDEX IF NOT EXISTS idx_empresa_natureza ON empresa(natureza_juridica)',
            'CREATE INDEX IF NOT EXISTS idx_socios_cpf ON socios(cpf_cnpj_socio)',
            ]

            for index in additional_indexes:
                try:
                    logging.info("Criando índice: %s", index)
                    cursor.execute(index)
                    conexao.commit()
                except mysql_errors.Error as e:
                    logging.warning("Não foi possível criar índices %s: %s", index, e)
                    continue
        finally:
            if cursor:
                cursor.close()

    def run(self):
        """Executa o processo completo de carga""" 
        total_start = time.time()

        try:
            logging.info("Iniciando processo local de carga de dados")

            # 1. Ler arquivos locais
            logging.info("Lendo arquivos locais...")
            self.files = self.get_local_zip_files()

            if not self.files:
                logging.warning("Nenhum arquivo ZIP encontrado para processamento. Verifique o diretório de entrada.")
                return
            
            # 2. Resetar diretório de extração (limpar arquivos antigos)
            self.file_processor.reset_extract_dir()

            # 3. Extrair arquivos
            logging.info("Extraindo arquivos...")
            self.file_processor.extract_files(self.files, Config.EXTRACT_DIR)

            # 4. Listar arquivos extraidos
            all_files = [
                f.name for f in Config.EXTRACT_DIR.iterdir() 
                if f.is_file()
            ]
            logging.info("Listando arquivos extraidos: %d", len(all_files))

            # 5. Categorizar arquivos
            logging.info("Categorizando arquivos...")
            categorizado_files = self.data_processor.categorize_files(all_files)
                
            # 6. Processar dados
            logging.info("Processando dados...")
            process_start = time.time()
            self.process_data_files(categorizado_files)

            process_time = time.time() - process_start

            logging.info("Tempo de processamento: %.2f segundos", process_time)

            # 7. Criar índices
            logging.info("Criando índices...")
            conexao_indices = self.db_manager.connect()
            try:
                self.create_indexes(conexao_indices)
            finally:
                if conexao_indices and conexao_indices.is_connected():
                    conexao_indices.close()            
            
            total_time = time.time() - total_start
            logging.info("Processo de carga concluído em %.2f segundos", total_time)

        except Exception as e:
            logging.error("Erro durante execução: %s", e)
            raise

                

def main():
    """Função principal."""
    loader = RFBDataLoader('F:\\Repositorio\\15_Git\\DADOS_RFB\\code')
    loader.run()

if __name__ == "__main__":
    main()