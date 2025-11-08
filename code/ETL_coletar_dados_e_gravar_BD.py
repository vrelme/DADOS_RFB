"""
Script para processamento de dados públicos do CNPJ da Receita Federal do Brasil.

Desenvolvido por: Aphonso Henrique do Amaral Rafael
Adaptado por: Vander Ribeiro Elme

"""

import os
import time
import logging
import hashlib
import zipfile
import csv
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import urljoin
from pathlib import Path
from decimal import Decimal
import contextlib

import requests
import mysql.connector
from mysql.connector import errors as mysql_errors
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import wget
import numpy as np

class Config:
    """Configurações da aplicação."""
    
    # Configuração do logging
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOG_FILE =  'DADOS_RFB.log'

    # URLs base
    DADOS_RF_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-10/'

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
                    use_pure=True, 
                    auth_plugin='mysql_native_password',
                    charset='utf8mb4',
                    collation='utf8mb4_unicode_ci',
                    raise_on_warnings=True,
                    connection_timeout=30
                )

                logging.info("Conexão com o banco de dados estabelecida com sucesso")
                logging.info("Versão do banco: %s",self.connection.server_info)
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
    def check_remote_file_diff(url:str, local_path:Path) -> bool:
        """Verifica se o arquivo remoto é diferente do local."""
        try:
            response = requests.head(url, timeout=30)
            response.raise_for_status()

            remote_size = int(response.headers.get('content-length',0))
            local_path_obj = Path(local_path)

            if not local_path_obj.exists():
                logging.info("Arquivo local não existe: %s",local_path)
                return True
            
            if local_path_obj.stat().st_size != remote_size:
                logging.info("Tamanho diferente: local=%d, remoto=%d", local_path_obj.stat().st_size, remote_size)
                return True
            
            return False # Para performance, vamos pular a verificação de hash por enquanto
                
        except requests.RequestException as e:
            logging.error("Erro ao verificar arquivo remoto %s: %s", url, e)
            return True
        
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
        elif isinstance(value, (np.integer, np.int64)):
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
            logging.info("Variáveis de ambiente carregadas de: %s", dotenv_path)
        else:
            logging.warning("Ärquivo .env não encontrado em: %s", dotenv_path)

    def setup_directories(self) -> Tuple[Path, Path]:
        """Configura dirétorios de trabalho."""
        output_path = Path(os.getenv('OUTPUT_FILES_PATH','output'))
        extract_path = Path(os.getenv('EXTRACTED_FILES_PATH','estracted'))

        self.file_processor.create_directories(output_path, extract_path)
        return output_path, extract_path

    def fetch_file_list(self, url:str = Config.DADOS_RF_URL) -> List[str]:
        """Öntém lista de arquivos do site da RFB."""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            files = []

            for link in soup.find_all('a', href = True):
                href = link['href']
                if href.endswith('.zip'):
                    files.append(href)
                    logging.info('Arquivo encontrato: %s', href)

            return files
        except requests.RequestException as e:
            logging.error("Erro ao obter lista de  arquivos: %s", e)
            raise      
        
    def download_files(self, files:List[str], base_url: str = Config.DADOS_RF_URL) -> List[Path]:
        """Faz download dos arquivos."""
        downloaded_files = []

        for filename in files:
            file_url = urljoin(base_url, filename)
            local_path = self.output_dir / filename

            if not self.file_processor.check_remote_file_diff(file_url, local_path):
                logging.info("Ärquivo já está atualizado: %s", filename)
                downloaded_files.append(local_path)
                continue

            try:
                logging.info("Baixando: %s", filename)
                wget.download(file_url, out=str(self.output_dir), bar=self.file_processor.download_progress)
                downloaded_files.append(local_path)
                print() # Nova linha após a barra de progresso

            except Exception as e:
                logging.error("Erro ao baixar %s: %s", filename, e)
                raise
        return downloaded_files

    def process_data_files(self, categorizado_files: Dict[str, List[str]]):
        """Pocessa os arquivos de dados e insere no banco de dados."""
        # Usar uma única conexào para todo processo
        conexao = None

        try: 
            conexao = self.db_manager.connect()
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
                    cnpj_basico VARCHAR(14),
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
                    cnpj_basico VARCHAR(14),
                    cnpj_ordem VARCHAR(4),
                    cnpj_dv VARCHAR(2),
                    identificador_matriz_filial INT,
                    nome_fantasia VARCHAR(255),
                    situacao_cadastral INT,
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
                'columns':['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral',  'data_situacao_cadastral',
                           'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                           'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 
                           'telefone_2', 'dd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
            },
            'simples': {
                'schema': """CREATE TABLE simples (
                    cnpj_basico VARCHAR(14),
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
                        cnpj_basico VARCHAR(14),
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
                logging.info(f"Tabela {table_name} já existia e será recriada.")
                # Drop e criação da tabela, conforme configuração
                logging.warning(f"ATENÇÃO: A tabela {table_name} será removida e recriada. Todos os dados existentes serão perdidos.")
                logging.info(f"Recriando tabela: {table_name}...")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")    # ← DROP se existir
                logging.info(f"Tabela {table_name} não existia e será criada.")
            
            else:
                logging.info(f"Configuração ativa: NÃO remover a tabela {table_name}. Dados existentes serão mantidos.")
                # Executar CREATE TABLE com tratamento de erro
                try:
                    cursor.execute(table_definition['schema'])              # ← CREATE TABLE
                    conexao.commit()
                    logging.info(f"Tabela {table_name} criada com sucesso.")
                except mysql_errors.Error as e:
                    logging.error(f"Erro ao criar tabela {table_name}: {e}")
                    # Mostrar o SQL que está causando erro para debugging
                    logging.error(f"SQL executado: {table_definition['schema']}")
                    conexao.rollback()
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
            cursor = conexao.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = '{table_name}'
            """)
            resultado = cursor.fetchone()
            return resultado[0] > 0
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
            dtype_spec = self.get_optimized_dtypes(column_names)
        
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
                quoting=csv.QUOTE_NONE
            ):
                chunk_number += 1
                # Aplicar transformações otimizadas
                chunk = self.apply_optimized_transformations(chunk, column_names)

                # Converter para tipos nativos Python
                data = self.convert_chunk_to_native_types(chunk)

                # Inserir dados no banco
                if data:
                    self.batch_insert_data_optimized(conexao, table_name, data, column_names, chunk_number)
                    total_rows += len(data)

                logging.info("Processadas %d linhas da tabela %s (chunk %d)", 
                             total_rows, table_name, chunk_number + 1)
            
                logging.info("Total de %d linhas processadas para tabela %s", total_rows, table_name)

        except Exception as e:
            logging.error("Erro ao processar arquivo %s: %s", file_path, e)
            raise

    def get_optimized_dtypes(self, column_names: List[str]) -> Dict[str, str]:
        """Retorna mapeamento de tipos otimizados para pandas."""
        dtype_map = {}
        for col in column_names:
            if 'cnpj' in col.lower() or col in ['cpf_cnpj_socio', 'cnpj_ordem', 'cnpj_dv']:
                dtype_map[col] = 'string'
            elif col in ['capital_social']:
                dtype_map[col] = 'string'  # Converter depois
            elif any(keyword in col for keyword in ['natureza_juridica', 'qualificacao', 'identificador', 'situacao', 'motivo',
                                      'porte_empresa', 'codigo', 'municipio', 'pais', 'faixa_etaria']):
                dtype_map[col] = 'Int32'
            elif any(keyword in col for keyword in ['data', 'date']):
                dtype_map[col] = 'string'  # Será convertido para datetime
            else:
                dtype_map[col] = 'string'
        return dtype_map
    
    def get_date_columns(self, column_names: List[str]) -> List[str]:
        """Identifica colunas de data para parsing automático."""
        date_columns = []
        for col in column_names:
            if any(keyword in col for keyword in ['data', 'date']):
                date_columns.append(col)
        return date_columns

    def apply_optimized_transformations(self, df: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:
        """Aplica transformações otimizadas nos dados."""
        for col in column_names:
            if col not in df.columns:
                continue

            try:
                if col == 'capital_social':
                    # Converter capital social para numérico
                    df[col] = pd.to_numeric(
                        df[col].str.replace(',', '.', regex=False), 
                        errors='coerce'
                    )
                elif col in ['natureza_juridica', 'qualificacao_responsavel', 'identificador_matriz_filial', 'situacao_cadastral', 
                             'motivo_situacao_cadastral', 'porte_empresa', 'municipio', 'pais', 'faixa_etaria', 'codigo']:
                    
                    # Converter para Int32 (nullable integer)
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
                elif any(keyword in col for keyword in ['data', 'date']):
                    # Já foi convertido pelo parse_dates
                    continue
                    
            except Exception as e:
                logging.warning("Erro ao transformar coluna %s: %s", col, e)
                continue
        
        return df

    def convert_chunk_to_native_types(self, chunk: pd.DataFrame) -> List[Tuple]:
        """Converte chunk do pandas para lista de tuplas com tipos nativos."""
        data = []
        for row in chunk.itertuples(index=False, name=None):
            converted_row = tuple(self.data_processor.convert_to_native_types(value) for value in row)
            data.append(converted_row)
        return data

    def batch_insert_data_optimized(self, conexao: mysql.connector.MySQLConnection,
                                   table_name: str, data: List[Tuple],
                                   column_names: List[str]):
        """Insere dados em lotes com otimizações."""
        if not data:
            return
        
        cursor = None

        try:
            cursor = conexao.cursor()
            columns = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            try:
                # Inserir em lotes menores para evitar timeouts
                batch_size = min(Config.BATCH_SIZE, 10000)
                total_inserted = 0
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    cursor.executemany(insert_query, batch)
                    conexao.commit()
                    total_inserted += len(batch)
                    
                    logging.info("Inserido lote de %d registros em %s (total: %d)", 
                               len(batch), table_name, total_inserted)
                
            except mysql_errors.Error as e:
                logging.error("Erro ao inserir dados em %s: %s", table_name, e)
                if cursor:
                    conexao.rollback()
                raise
            finally:
                if cursor:
                    cursor.close()
        except mysql_errors.Error as e:
                # Tentar inserir em lotes menores em caso de erro
                self.batch_insert_data_optimized(conexao, table_name, data, column_names)

    def insert_in_smaller_batches(self, conexao: mysql.connector.MySQLConnection,
                                 table_name: str, data: List[Tuple],
                                 column_names: List[str], cursor):
        """Insere dados em lotes menores em caso de erro."""
        cursor = None

        try:
            cursor = conexao.cursor()
            batch_size = 1000
            columns = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
            successful_inserts = 0
        
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                try:
                    cursor.executemany(insert_query, batch)
                    conexao.commit()
                    successful_inserts += len(batch)
                    logging.info("Inserido sub-lote de %d registros em %s", len(batch), table_name)
                except mysql_errors.Error as e:
                    logging.error("Erro em sub-lote %s: %s", table_name, e)
                    conexao.rollback()
                    continue
            logging.error("Total de registros inseridos com sucesso em %s: %d", 
                         table_name, successful_inserts)
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
            'CREATE INDEX idx_estabelecimento_uf ON estabelecimento(uf)',
            'CREATE INDEX idx_establecimento_municipio ON estabelecimento(municipio)',
            'CREATE INDEX idx_establecimento_cnae ON estabelecimento(cnae_fiscal_principal)',
            'CREATE INDEX idx_empresa_natureza ON empresa(natureza_juridica)',
            'CREATE INDEX idx_socios_cpf ON socios(cpf_cnpj_socio)',
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
            logging.info("Iniciando processo de carga de dados da RFB")

            # 1. Obter lista de arquivos
            logging.info("Obtendo lista de arquivos...")
            self.files = self.fetch_file_list()

            # 2. Download dos arquivos
            logging.info("Iniciando download dos arquivos...")
            downloaded_files = self.download_files(self.files)

            # 3. Extrair arquivos
            logging.info("Extraindo arquivos...")
            self.file_processor.extract_files(downloaded_files, self.extract_dir)

            # 4. Listar e categorizar arquivos
            logging.info("Categorizar arquivos...")
            all_files = [f.name for f in self.extract_dir.iterdir() if f.is_file()]
            categorizado_files = self.data_processor.categorize_files(all_files)
                
            # 5. Processar dados
            logging.info("Processando dados...")
            process_start = time.time()
            self.process_data_files(categorizado_files)
            process_time = time.time() - process_start
            logging.info("Tempo de processamento: %.2f segundos", process_time)

            # 6. Criar índices
            logging.info("Criando índices...")
            index_start = time.time()
            conexao_indices = self.db_manager.connect()
            try:
                self.create_indexes(conexao_indices)
            finally:
                if conexao_indices and conexao_indices.is_connected():
                    conexao_indices.close()

            index_time = time.time() - index_start
            logging.info("Tempo para criar índices: %.2f segundos", index_time)

            total_time = time.time() - total_start
            logging.info("Processo de carga concluído em %.2f segundos", total_time)
            logging.info("Processo 100 %% finalizado")

        except Exception as e:
            logging.error("Erro durante execução: %s", e)
            raise
        finally:
            if self.db_manager.connection and self.db_manager.connection.is_connected():
                self.db_manager.connection.close()
                

def main():
    """Função principal."""
    loader = RFBDataLoader('F:\\Repositorio\\00_Programacao\\08-DADOS_RFB\\DADOS_RFB\\code')
    loader.run()

if __name__ == "__main__":
    main()