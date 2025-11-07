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
            if not self.connection or not self.connection.is_connected()    :
                self.connect()

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
            # Verificar hash se os tamanhos forem iguais 
            local_hash = FileProcessor.calculate_file_hash(local_path_obj)
            remote_hash = FileProcessor.calculate_remote_file_hash(url)

            if local_hash != remote_hash:
                logging.info("Hash diferente para arquivo: %s", local_path)
                return True
            
            return False
        
        except requests.RequestException as e:
            logging.error("Erro ao verificar arquivo remoto %s: %s", url, e)
            return True

    @staticmethod
    def calculate_file_hash(file_path:Path) -> str:
        """Calcula hash SHA256 de um arquivo local."""
        sha256_hash = hashlib.sha256()
        with open (file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096),b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    @staticmethod
    def calculate_remote_file_hash(url:str) -> str:
        """ Calcula hash SHA256 de um arquivo remoto. """
        sha256_hash = hashlib.sha256()
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        for chunk in response.iter_content(chunk_size=4096):
            sha256_hash.update(chunk)

        return sha256_hash.hexdigest()

    @staticmethod
    def download_progress(current: int, total: int, width: int=80):
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
    def parse_date(date_str:str) -> Optional[pd.Timestamp]:
        """"Converte string de data para objeto datetime."""
        if not date_str or not date_str.strip() or not date_str.isdigit():
            return None
        try:
            return pd.to_datetime(date_str, format='%Y%m%d')
        except ValueError:
            return None
        
    @staticmethod
    def clean_numeric_value(value:str, field_name: str) -> Any:
        """Limpa e converte valores numéricos."""
        if not value or not value.strip():
            return None
        
        if field_name == 'capital_social':
            return float(value.replace(',','.')) if value.replace(',','').replace('.','').isdigit() else None
        elif field_name in ['natureza_juridica', 'qualificacao_responsavel', 'ídentificador_matriz_filial', 'situacao_cadsatral']:
            return int(value) if value.isdigit() else None
        
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
        
    def download_files(self, files:List[str], base_url: str = Config.DADOS_RF_URL):
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
        conexao = self.db_manager.connect()

        table_definitions = self.get_table_definitions()

        for table_name, file_list in categorizado_files.items():
            if not file_list:
                logging.warning("Nenhum arquivo encontrado para tabela: %s", table_name)
                continue

            logging.info("Processando tanela: %s", table_name)
            self.process_table_data(conexao, table_name, file_list, table_definitions[table_name])

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
                    ente_federativo_responsavel VARCHAR(255)
                )""".replace('\n                    ',''),
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
                    data_situacao_especial DATE
                )""".replace('\n                    ',''),
                'columns':['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
                           'nome_fantasia','situacao_cadastral', 'data_situacao_cadastral',
                           'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais','data_inicio_atividade',
                           'cnae_fiscal_principal','cnae_fiscal_secundaria','tipo_logradouro','logradouro',
                           'numero','complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
                           'ddd_2', 'telefone_2', 'dd_fax', 'fax','correio_eletronico', 'situacao_especial',
                           'data_situacao_especial']

            },
            'simples': {
                'schema': """CREATE TABLE simples (
                    cnpj_basico VARCHAR(14),
                    opcao_simples VARCHAR(1),
                    data_opcao_simples DATE,
                    data_exclusao_simples DATE,
                    opcao_mei VARCHAR(3),
                    data_opcao_mei DATE,
                    data_exclusao_mei DATE
                )""".replace('\n                ', ' '),
                'columns': ['cnpj_basico', 'opcao_simples', 'data_opcao_simples', 
                            'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 
                            'data_exclusao_mei']
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
                        faixa_etaria INT
                    )""".replace('\n                ', ' '),
                    'columns': ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 
                                'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada_sociedade', 
                                'pais', 'representante_legal', 'nome_do_representante', 
                                'qualificacao_representante_legal', 'faixa_etaria']
            },
            'pais': {
                'schema': """CREATE TABLE pais (
                    codigo INT,
                    nome VARCHAR(255)
                )""".replace('\n                ', ' '),
                'columns': ['codigo', 'nome']
            },
            'munic': {
                'schema': """CREATE TABLE munic (
                    codigo INT,
                    nome VARCHAR(255)
                )""".replace('\n                ', ' '),
                'columns': ['codigo', 'nome']
            },
            'quals': {
                'schema': """CREATE TABLE quals (
                    codigo INT,
                    nome VARCHAR(255)
                )""".replace('\n                ', ' '),
                'columns': ['codigo', 'nome']
            },
            'natju': {
                'schema': """CREATE TABLE natju (
                    codigo INT,
                    nome VARCHAR(255)
                )""".replace('\n                ', ' '),
                'columns': ['codigo', 'nome']
            },
            'cnae': {
                'schema': """CREATE TABLE cnae (
                    codigo INT,
                    nome VARCHAR(255)
                )""".replace('\n                ', ' '),
                'columns': ['codigo', 'nome']
            }
        }

    def process_table_data(self,conexao:mysql.connector.MySQLConnection,
                           table_name:str, file_list: List[str],
                           table_definition: Dict):
        """Processa dados de uma tabela específica."""
        cursor = None
        try:
            cursor = conexao.cursor()
            db_name = os.getenv('DB_NAME')

            # Garantir que estamos usando o banco de dados correto
            cursor.execute(f"USE {db_name}")

             # Nome completo da tabela
            full_table_name = f"{db_name}.{table_name}"

            # Verificar se a tabela existe
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{db_name}'
                AND table_name = '{table_name}'
            """)

            table_exists = cursor.fetchone()[0] > 0

            if table_exists:
                # Se existir, fazer drop
                logging.info(f"Removendo tabela existente {full_table_name}...")
                cursor.execute(f'DROP TABLE IF EXISTS {full_table_name}')

            # Drop e criação da tabela
            logging.info(f"Criando tabela {full_table_name}...")
            cursor.execute(table_definition['schema'])  
            conexao.commit()

            # Processar arquivos
            for filename in file_list:
                file_path = self.extract_dir / filename
                logging.info("Processando arquivo: %s", filename)

                self.process_single_file(conexao, full_table_name, file_path, 
                                         table_definition['columns'])
        except mysql_errors.Error as e:
            logging.error("Erro ao processar tabela %s: %s", table_name, e)
            raise
        finally:
            if cursor:
                cursor.close()

    def process_single_file(self, conexao:mysql.connector.MySQLConnection,
                            table_name:str, file_path:Path,
                            column_names:List[str]):
        """Processa um único arquivo usando pandas para melhor performance."""
        try:
            # Usando pandas com dtype apropriado para melhor performance
            dtype_dict = self.get_dtype_mapping(column_names)

            # Ler arquivo em chunks para economizar memória
            chunk_size = 50000
            total_rows = 0

            for chunk in pd.read_csv(file_path,
                                    sep=';', 
                                    encoding='latin1', 
                                    names=column_names,
                                    header=None,
                                    dtype=dtype_dict,
                                    chunksize=chunk_size, 
                                    low_memory=False
                            ):
                # Aplicar transformações 
                chunk = self.apply_data_transformations(chunk, column_names)

                # Substituir NaN por None para compatibilidade com MySQL
                chunk = chunk.where(pd.notnull(chunk), None)
                def to_native(value):
                    if value is None:
                        return None
                    # pandas  Timestamp / numpy datetime64 -> python datetime
                    if isinstance(value, (pd.Timestamp, np.datetime64)):
                        try:
                            return pd.Timestamp(value).to_pydatetime()
                        except Exception:
                            return str(value)
                    if isinstance(value, (np.integer,)):
                        return int(value) 
                    if isinstance(value, (np.floating,)):
                        return float(value)
                    if isinstance(value, (np.bool_, bool)):
                        return bool(value)
                    if isinstance(value, (bytes, bytearray)):
                        try:
                            return value.decode('utf-8')
                        except Exception:
                            return str(value)
                    # catch-all para outros tipos
                    if isinstance(value, np.generic):
                        return value.item()
                    return value

                # Converter para lista de tuplas para inserção
                data = []
                for row in chunk.itertuples(index=False, name=None):
                    data.append(tuple(to_native(v) for v in row))

                # Inserir dados no banco
                self.batch_insert_data(conexao, table_name, data, column_names)
                total_rows += len(data)

                logging.info("Insiridas %d linhas da tabela %s", total_rows, table_name)
            
        except Exception as e:
            logging.error("Erro ao processar arquivo %s: %s", file_path, e)
            raise
        
    def get_dtype_mapping(self, column_names:List[str]) -> Dict[str, str]:
        """Retorna mapeamento de tipos para pandas."""
        dtype_map = {}
        for col in column_names:
            if col in ['capital_social']:
                dtype_map[col] = 'object'  # Será convertido depois
            elif any(keyword in col for keyword in ['data', 'date']):
                dtype_map[col] = 'object'  # Será convertido depois
            else:
                dtype_map[col] = 'object'  # Usar object para evitar problemas
        return dtype_map

    def apply_data_transformations(self, df:pd.DataFrame, column_names:List[str]) -> pd.DataFrame:
        """Aplica transformações nos Dados.""" 
        for col in column_names:
            # Verifica se a coluna existe no DataFrame
            if col not in df.columns:
                logging.warning(f"Coluna {col} não encontrada no DataFrame")
                continue

            try:
                if col in ['capital_social']:
                    # Substituir vírgulas por pontos e converter para numérico
                    df[col] = df[col].apply(lambda x: str(x).replace(',','.') if pd.notna(x) else None)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif any(keyword in col for keyword in ['data', 'date']):
                    df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                elif col in ['natureza_juridica', 'qualificacao_responsavel',
                         'identificador_matriz_filial',
                         'situacao_cadastral']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
            except Exception as e:
                logging.error(f"Erro ao transformar coluna %s: %s", col, e)
                continue       
        return df
    
    def batch_insert_data(self, conexao:mysql.connector.MySQLConnection,
                          table_name:str, data:List[Tuple],
                          column_names:List[str], batch_size:int = Config.BATCH_SIZE):

        """Insere dados em lotes"""
        if not data:
            return
        
        cursor = None
        try:
            self.db_manager.ensure_connection()
            cursor = conexao.cursor()

            columns = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                conexao.commit()

                logging.info(f"Inserido lote de {len(batch)} registros em {table_name}")
        
        except mysql_errors.Error as e:
            logging.error(f"Erro ao inserir dados em {table_name}: {e}")
            if cursor:
                conexao.rollback()
                raise
        finally:
            if cursor:
                cursor.close()

    def create_indexes(self, conexao:mysql.connector.MySQLConnection):
        """Cria índices no banco de dados."""
        indexes = [
            'CREATE INDEX empresa_cnpj ON empresa(cnpj_basico)',
            'CREATE INDEX estabelecimento_cnpj ON estabelecimento(cnpj_basico)',
            'CREATE INDEX socios_cnpj ON socios(cnpj_basico)',
            'CREATE INDEX simples_cnpj ON simples(cnpj_basico)'
        ]

        cursor = None
        try:
            cursor = conexao.cursor()
            for index in indexes:
                logging.info("Criando índice: %s", index)
                cursor.execute(index)
            conexao.commit()
        except mysql_errors.Error as e:
            logging.error("Erro ao criar índices: %s", e)
            raise
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
            self.create_indexes(self.db_manager.connection)
            index_time = time.time() - index_start
            logging.info("Tempo para criar índices: %.2f segundos", index_time)

            total_time = time.time() - total_start
            logging.info("Processo de carga concluído em %.2f segundos", total_time)
            logging.info("Processo 100 %% finalizado")

        except Exception as e:
            logging.error("Erro durante execução: %s", e)
            raise

def main():
    """Função principal."""
    loader = RFBDataLoader('F:\\Repositorio\\00_Programacao\\08-DADOS_RFB\\DADOS_RFB\\code')
    loader.run()

if __name__ == "__main__":
    main()