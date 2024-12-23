from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError, TimeoutError
import bs4 as bs
import csv
import dask.dataframe as dd
import getenv
import hashlib
import logging
import lxml
import mysql.connector
import numpy as np
import os
import pandas as pd
import re
import requests
import sql
import sqlalchemy
import sys
import time
import urllib.parse
import urllib.request
import wget
import zipfile

# Configuração do logging
logging.basicConfig(filename='DADOS_RFB.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
def main():
    cnpj_basico=''
    current = 1
    i=0
    # Gerar Log
    logging.info(f"Iniciando o processo de carga")
def check_diff(url, file_name):
    # Verifica se o arquivo local é idêntico ao arquivo remoto.
    #
    # Args:
    #    url (str): URL do arquivo remoto.
    #   file_name (str): Nome do arquivo local.
    #
    # Returns:
    #    bool: True se o arquivo precisa ser baixado, False caso contrário.
    #
    try:
        logging.info(f"Verificar diferença entre o arquivo local e o arquivo remoto")
        response = requests.head(url)
        response.raise_for_status()
    
        remote_size = int(response.headers.get('content-length', 0))
        if os.path.exists(file_name):
            with open(file_name, 'rb') as f:
                local_hash = hashlib.sha256(f.read()).hexdigest()

            # Verificar se o arquivo remoto é diferente do arquivo local    
            if os.path.getsize(file_name) != remote_size:
                logging.info(f"O arquivo remoto: {remote_size} é diferente do arquivo local")
                return True
            
            # Verificar se o hash do arquivo remoto é diferente do arquivo local
            response = requests.get(url)
            response.raise_for_status()
            remote_hash = hashlib.sha256(response.content).hexdigest()
            if local_hash != remote_hash:
                logging.info(f"O hash do arquivo remoto é diferente do arquivo local")
                return True
        
        return False
    
    except requests.RequestException as e:
        logging.error(f"Erro ao verificar o arquivo remoto: {e}")
        print(f"Erro ao verificar o arquivo remoto: {e}")
        return True
    
    except OSError as e:
        logging.error(f"Erro ao acessar o arquivo local: {e}")
        print(f"Erro ao acessar o arquivo local: {e}")
        return True
def create_dataframe(data, columns):
    """Cria um DataFrame a partir dos dados e colunas fornecidos.

    Args:
    data: Uma lista de listas ou um dicionário contendo os dados.
    columns: Uma lista com os nomes das colunas.

    Returns:
    Um DataFrame dask.
    """
    logging.info(f"Criar DataFrame")
    df = dd.DataFrame(data, columns=columns)
    return df
def makedirs_custom(path, exist_ok=True, mode=0o755):
    """Cria diretórios recursivamente com controle de existência e permissões.

    Args:
        path (str): Caminho completo do diretório a ser criado.
        exist_ok (bool, opcional): Se True, não gera erro se o diretório já existe.
        mode (int, opcional): Permissões do diretório.

    Returns:
        bool: True se o diretório foi criado com sucesso, False caso contrário.
    """
    logging.info(f"Criar diretório")
    try:
        os.makedirs(path, exist_ok=exist_ok, mode=mode)
        logging.info(f"Diretório {path} criado com sucesso")
        return True
    except OSError as e:
        logging.error(f"Erro ao criar diretório {path}: {e}")
        return False
def process_and_insert_chunk(df_chunk, conexao, table_name):
    """
        Grava dados no banco de dados.

        Args:
        df_chunk (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        conexao (sqlalchemy.engine.base.Engine): Conexão com o banco de dados.
        table_name (str): Nome da tabela no banco de dados.
    """
    logging.info(f"Gravar dados no banco {table_name}")
    try:
        connection_uri = f"mysql+mysqlconnector://{os.getenv('db_user')}:{os.getenv('db_password')}@{os.getenv('db_host')}:{os.getenv('DB_PORT')}/{os.getenv('db_name')}"         
        logging.info(f'URI de conexao: {connection_uri}')

        # Tentar conectar ao banco de dados
        try:
            engine = sqlalchemy.create_engine(connection_uri)
            logging.info(f'Engine criado com sucesso para {table_name}')
        except Exception as e:
            logging.error(f"Erro ao criar Engine para {table_name}: {e}")
            return

        # Verificar se a conexão foi estabelecida
        try:

            logging.info(f"Estabelecer conexão com o banco de dados {table_name}")
            with engine.connect() as connection:
                logging.info(f"Conexão com o banco de dados {table_name} estabelecida com sucesso")
                
                # verificar se a tabela existe
                inspector = inspect(engine)
                if not inspector.has_table(table_name):
                    logging.info(f"Tabela {table_name} não existe. Criando tabela.")
                    df_chunk.head(0).to_sql(table_name, engine, if_exists='replace', index=False)    
                    logging.info(f"Tabela {table_name} criada com sucesso")

                # Processo para inserir os dados no banco
                logging.info(f"Processo para inserir os dados no banco {table_name}")
                df_chunk.to_sql(table_name, engine, if_exists='append', index=False)
                logging.info(f"Tabela: {table_name} inserido com sucesso no banco de dados")

        except TimeoutError as e:
            logging.error(f"Erro ao timeout ao banco de dados {table_name}: {e}")
            print(f"Erro ao timeout ao banco de dados {table_name}: {e}")
            return
        except OperationalError as e:
            logging.error(f"Erro ao operacional ao banco de dados {table_name}: {e}")
            print(f"Erro ao operacional ao banco de dados {table_name}: {e}")
            return
        except Exception as e:
            logging.error(f"Erro ao conectar ao banco de dados {table_name}: {e}")
            print(f"Erro ao conectar ao banco de dados {table_name}: {e}")
            return
    except Exception as e:
        logging.error(f"Erro ao inserir dados na tabela {table_name}: {e}")
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")
    finally:
        logging.info(f"Finalizando processo de inserção de dados na tabela {table_name}")
def getEnv(env):
    """
    Retorna o valor de uma variável de ambiente.

    Args:
        env (str): Nome da variável de ambiente.

    Returns:
        str: Valor da variável de ambiente.
    """
    logging.info(f"Retornar valor da variável de ambiente")
    return os.getenv(env)
def load_enviroment(local_env):
    """Carrega as variáveis de ambiente de um arquivo .env.

    Args:
        dotenv_path (str): Caminho completo do arquivo .env.

    Returns:
        bool: True se o arquivo foi carregado com sucesso, False caso contrário.
    """
    logging.info(f"Carregar variáveis de ambiente")
    dotenv_path = os.path.join(local_env, '.env')
    load_dotenv(dotenv_path=dotenv_path)
    logging.info(f"Variáveis de ambiente carregadas")
def define_directories():
    """Define os diretórios de saída e extração dos arquivos.

    Returns:
        str, str: Caminho dos diretórios de saída e extração dos arquivos.
    """
    try:
        logging.info(f"Definir diretórios")
        output_files = getEnv('OUTPUT_FILES_PATH')
        makedirs_custom(output_files, True, 0o755)
        extracted_files = getEnv('EXTRACTED_FILES_PATH')
        makedirs_custom(extracted_files, True, 0o755)

        logging.info(f"Diretórios definidos: output_files: {output_files}, extracted_files: {extracted_files}")
        print('Diretórios definidos: \n' + 'output_files: ' + str(output_files) + '\n' + 'extracted_files: ' + str(extracted_files))
        return output_files, extracted_files
    except OSError as e:
        logging.error(f"Erro na definição dos diretórios, verifique o arquivo '.env' ou o local informado do seu arquivo de configuração: {e}")
        raise
def fetch_data(url):
    """Obtém os dados de uma URL.

    Args:
        url (str): URL dos dados.

    Returns:
        str: Dados obtidos da URL.
    """
    try:
        logging.info(f"Obter dados da URL")
        response = urllib.request.urlopen(url)
        raw_html = response.read()
        return raw_html
    except Exception as e:
        logging.error(f"Erro ao obter dados da URL {url}: {e}")
        print(f"Erro ao obter dados da URL {url}: {e}")
        raise
def extract_files_from_html(html_str, file_extension='.zip'):
    """Extrai os arquivos de uma string HTML.

    Args:
        html_str (str): String HTML.
        file_extension (str): Extensão do arquivo a ser procurado.
    Returns:
        list: Lista de arquivos extraídos.
    """
    try:
        logging.info(f"Extrair arquivos")
        page_items = bs.BeautifulSoup(html_str, 'lxml')
        html_str = str(page_items)

        files = []
        for m in re.finditer(file_extension, html_str):  # type: ignore
            i_start = m.start() - 40
            i_end = m.end()
            file_snippet = html_str[i_start:i_end]
            # Limpar o nome do arquivo
            file_name = re.search(r'href="([^"])"', file_snippet)
            if file_name:
                files.append(file_name.group(1)) 
        return files

    except Exception as e:
        logging.error(f"Erro ao extrair arquivos da string HTML: {e}")
        print(f"Erro ao extrair arquivos da string HTML: {e}")
        raise
def delete_files_variable():
    """Deleta a variável Files.

    Returns:
        bool: True se a variável foi deletada com sucesso, False caso contrário.
    """
    try:
        logging.info(f"Deletar variável Files")
        del files
        return True
    except Exception as e:
        logging.error(f"Erro ao deletar a variável Files: {e}")
        print(f"Erro ao deletar a variável Files: {e}")
        return False
def print_files_list(files):
    """Imprime a lista de arquivos.

    Args:
        Files (list): Lista de arquivos.
    """
    try:
        logging.info(f"Imprimir lista de arquivos")
        for i, f in enumerate(Files, start=1):
            print(f'{i} - {f}') 
    except Exception as e:
        logging.error(f"Erro ao imprimir a lista de arquivos: {e}")
        print(f"Erro ao imprimir a lista de arquivos: {e}")
def bar_progress(current, total, width=80):
    messagem = "Executando: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
    # Don't use print() as it will print in new line every time.
    sys.stdout.write("\r" + messagem)
    sys.stdout.flush()
def download_files(Files, base_url, output_files):
    """Baixa os arquivos de uma lista.

    Args:
        Files (list): Lista de arquivos.
        output_files (str): Caminho do diretório de saída dos arquivos.
    """
    try:
        logging.info(f"Baixar arquivos")
        print('Baixando arquivo:')
        for i, file in enumerate(Files, start=1):
            print(f'{i} - {file}')
            url = f'{base_url}/{file}'
            file_name = os.path.join(output_files, file)
            if check_diff(url, file_name):
                try:
                    wget.download(url, out=output_files, bar=bar_progress)
                except Exception as e:
                    logging.error(f"Erro ao baixar o arquivo {file}: {e}")
                    print(f"Erro ao baixar o arquivo {file}: {e}")
        logging.info(f"Fim do Download doa arquivos")

    except Exception as e:
        logging.error(f"Erro ao baixar os arquivos: {e}")
        print(f"Erro ao baixar os arquivos: {e}")
def extract_files(Files, output_files, extracted_files):
    """Descompacta os arquivos de uma lista.

    Args:
        Files (list): Lista de arquivos.
        output_files (str): Caminho do diretório de saída dos arquivos.
        extracted_files (str): Caminho do diretório de extração dos arquivos.
    """
    logging.info(f"Descompactar arquivos")
    print('Descompactando arquivo:')
    for i, file in enumerate(Files, start=1):
        try:
            print(f'{i} - {file}')
            full_path = os.path.join(output_files, file)
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_files)
        except Exception as e:
            logging.error(f"Erro ao descompactar os arquivos: {e}")
                
    logging.info(f"Fim descompactando arquivo")
def separar_arquivos(items):
    """Separa os arquivos em listas de acordo com o nome.

    Args:
        items (list): Lista de arquivos.

    Returns:
        list: Listas de arquivos separados.
    """
    try:
        logging.info(f"Separar arquivos")
        arquivos = {
            'empresa': [],
            'estabelecimento': [],
            'socios': [],
            'simples': [],
            'cnae': [],
            'moti': [],
            'munic': [],
            'natju': [],
            'pais': [],
            'quals': []
        }
        for item in items:
            if 'EMPRE' in item:
                arquivos['empresa'].append(item)
            elif 'ESTABELE' in item:
                arquivos['estabelecimento'].append(item)
            elif 'SOCIO' in item:
                arquivos['socios'].append(item)
            elif 'SIMPLES' in item:
                arquivos['simples'].append(item)
            elif 'CNAE' in item:
                arquivos['cnae'].append(item)
            elif 'MOTI' in item:
                arquivos['moti'].append(item)
            elif 'MUNIC' in item:
                arquivos['munic'].append(item)
            elif 'NATJU' in item:
                arquivos['natju'].append(item)
            elif 'PAIS' in item:
                arquivos['pais'].append(item)
            elif 'QUALS' in item:
                arquivos['quals'].append(item)
            
        return arquivos
    except Exception as e:
        logging.error(f"Erro ao separar os arquivos: {e}")
        print(f"Erro ao separar os arquivos: {e}")
        raise
def connect_to_database():
    """Conecta ao banco de dados.

    Returns:
        mysql.connector.connection.MySQLConnection: Conexão com o banco de dados.
    """
    try:
        logging.info(f"Conectar ao banco de dados")
        conexao = mysql.connector.connect(
            host=os.getenv('db_host'),
            user=os.getenv('db_user'),
            password=os.getenv('db_password'),
            database=os.getenv('db_name'),
            use_pure=True
        )
        cur = conexao.cursor()
        logging.info("Conexão com o banco de dados estabelecida com sucesso")
        return conexao, cur
    except mysql.connector.Error as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        print(f"Erro ao conectar ao banco de dados: {e}")
        raise
def cursor():
    """Cria um cursor para a conexão com o banco de dados.

    Returns:
        mysql.connector.cursor.MySQLCursor: Cursor para a conexão com o banco de dados.
    """
    try:
        logging.info(f"Criar cursor")
        conexao = connect_to_database()
        cur = conexao.cursor()
        return cur
    except mysql.connector.Error as e:
        logging.error(f"Erro ao criar cursor: {e}")
        print(f"Erro ao criar cursor: {e}")
        raise
def processar_arquivos_empresa(arquivos_empresa, extracted_files, conexao, cur):
    """Processa os arquivos de empresa.

    Args:
        arquivos_empresa (list): Lista de arquivos de empresa.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
        cur (mysql.connector.cursor.MySQLCursor): Cursor para executar comandos SQL.
    """
    try:
        empresa_insert_start = time.time()
        table_name = 'empresa'
        num_particoes = 10
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS empresa;')
        conexao.commit()
        column_names = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
        logging.info(f"Processar arquivos de empresa")
        for e in range(0, len(arquivos_empresa)):
            print(f'Trabalhando no arquivo: {arquivos_empresa[e]} [...]')
            logging.info(f'Trabalhando no arquivo: {arquivos_empresa[e]} [...]')
            extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                # Cria a engine do SQLAlchemy com a string de conexão
                df = dd.from_pandas(pd.DataFrame(column_data), npartitions=num_particoes)
                
                # Insere o DataFrame no banco de dados
                # Gravar dados no banco:
                process_and_insert_chunk(df, conexao, 'empresa')

            print('Arquivos de empresa finalizados!')
            empresa_insert_end = time.time()
            empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
            print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))  

    except mysql.connector.Error as e:
        logging.error(f"Erro ao processar os arquivos de empresa: {e}")
        print(f"Erro ao processar os arquivos de empresa: {e}")
        raise
def processar_arquivos_estabelecimento(arquivos_estabelecimento):
    """Processa os arquivos de estabelecimento.

    Args:
        arquivos_estabelecimento (list): Lista de arquivos de estabelecimento.
    """
    try:
        logging.info(f"Processar arquivos de estabelecimento")
        for e in range(0, len(arquivos_estabelecimento)):
            print('Trabalhando no arquivo: '+arquivos_estabelecimento[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_estabelecimento[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_estabelecimento[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                # Cria a engine do SQLAlchemy com a string de conexão
                df = dd.from_pandas(pd.DataFrame(column_data), npartitions=num_particoes)
                
            
                # Insere o DataFrame no banco de dados
                # Gravar dados no banco:

                process_and_insert_chunk(df, conexao,'estabelecimento')

            print('Arquivos de estabelecimento finalizados!')
            estabelecimento_insert_end = time.time()
            estabelecimento_Tempo_insert = round((estabelecimento_insert_end - estabelecimento_insert_start))
            print('Tempo de execução do processo de estabelecimento (em segundos): ' + str(estabelecimento_Tempo_insert))  

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de estabelecimento: {e}")
        print(f"Erro ao processar os arquivos de estabelecimento: {e}")
        raise
def processar_arquivos_socios(arquivos_socios):
    """Processa os arquivos de sócios.

    Args:
        arquivos_socios (list): Lista de arquivos de sócios.
    """
    try:
        logging.info(f"Processar arquivos de sócios")
        for e in range(0, len(arquivos_socios)):
            print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_socios[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
            socios = dd.read_csv(extracted_file_path,
                                sep=';',
                                # nrows=100,
                                skiprows=0,
                                header=None,
                                dtype='object',
                                encoding='latin1')
            # Renomear colunas
            socios.columns = column_names
            # Tratamento do arquivo antes de inserir na base:
            socios = socios.reset_index()
            del socios['index']

            # Gravar dados no banco:
            # socios
            for i in range(socios.npartitions):
                df_chunk = socios.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'socios')

            try:
                del socios
            except:
                pass
            print('Arquivos de socios finalizados!')
            socios_insert_end = time.time()
            socios_Tempo_insert = round((socios_insert_end - socios_insert_start))
            print('Tempo de execução do processo de sócios (em segundos): ' + str(socios_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de sócios: {e}")
        print(f"Erro ao processar os arquivos de sócios: {e}")
        raise
def processar_arquivos_simples(arquivos_simples):
    """Processa os arquivos de Simples Nacional.

    Args:
        arquivos_simples (list): Lista de arquivos de Simples Nacional.
    """
    try:
        logging.info(f"Processar arquivos de Simples Nacional")
        for e in range(0, len(arquivos_simples)):
            print('Trabalhando no arquivo: '+arquivos_simples[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_simples[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])
            simples = dd.read_csv(extracted_file_path,
                                sep=';',
                                # nrows=100,
                                skiprows=0,
                                header=None,
                                dtype='object',
                                encoding='latin1')
            # Renomear colunas
            simples.columns = column_names
            # Tratamento do arquivo antes de inserir na base:
            simples = simples.reset_index()
            del simples['index']
            # Gravar dados no banco:
            # simples
            for i in range(simples.npartitions):
                df_chunk = simples.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'simples')

            try:
                del simples
            except:
                pass

            print('Arquivos do simples finalizados!')
            simples_insert_end = time.time()
            simples_Tempo_insert = round((simples_insert_end - simples_insert_start))
            print('Tempo de execução do processo do Simples Nacional (em segundos): ' + str(simples_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de Simples Nacional: {e}")
        print(f"Erro ao processar os arquivos de Simples Nacional: {e}")
    processar_arquivos_cnae(arquivos['cnae'], extracted_files, conexao, cur)
def processar_arquivos_cnae(arquivos_cnae):
    """Processa os arquivos de CNAE.

    Args:
        arquivos_cnae (list): Lista de arquivos de CNAE.
    """
    try:
        logging.info(f"Processar arquivos de CNAE")
        for e in range(0, len(arquivos_cnae)):
            print('Trabalhando no arquivo: '+arquivos_cnae[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_cnae[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_cnae[e])
            cnae = dd.read_csv(extracted_file_path,
                                sep=';',
                                # nrows=100,
                                skiprows=0,
                                header=None,
                                dtype='object',
                                encoding='latin1')
            # Renomear colunas
            cnae.columns = column_names

            # Tratamento do arquivo antes de inserir na base:
            cnae = cnae.reset_index()
            del cnae['index']

            # Gravar dados no banco:
            # cnae
            for i in range(cnae.npartitions):
                df_chunk = cnae.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'cnae')

            try:
                del cnae
            except:
                pass
            print('Arquivos de cnae finalizados!')
            cnae_insert_end = time.time()
            cnae_Tempo_insert = round((cnae_insert_end - cnae_insert_start))
            print('Tempo de execução do processo de cnae (em segundos): ' + str(cnae_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de CNAE: {e}")
        print(f"Erro ao processar os arquivos de CNAE: {e}")
        raise
    processar_arquivos_moti(arquivos['moti'], extracted_files, conexao, cur)
def processar_arquivos_moti(arquivos_moti):
    """Processa os arquivos de motivos da situação atual.

    Args:
        arquivos_moti (list): Lista de arquivos de motivos da situação atual.
    """
    try:
        logging.info(f"Processar arquivos de motivos da situação atual")
        for e in range(0, len(arquivos_moti)):
            print('Trabalhando no arquivo: '+arquivos_moti[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_moti[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_moti[e])
            moti = dd.read_csv(extracted_file_path,
                                sep=';',
                                # nrows=100,
                                skiprows=0,
                                header=None,
                                dtype='object',
                                encoding='latin1')
            # Renomear colunas
            moti.columns = column_names

            # Tratamento do arquivo antes de inserir na base:
            moti = moti.reset_index()
            del moti['index']

            # Gravar dados no banco:
            # moti
            for i in range(moti.npartitions):
                df_chunk = moti.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'moti')

            try:
                del moti
            except:
                pass
            print('Arquivos de moti finalizados!')
            moti_insert_end = time.time()
            moti_Tempo_insert = round((moti_insert_end - moti_insert_start))
            print('Tempo de execução do processo de motivos da situação atual (em segundos): ' +
                str(moti_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de motivos da situação atual: {e}")
        print(f"Erro ao processar os arquivos de motivos da situação atual: {e}")
        raise
def processar_arquivos_munic(arquivos_munic):
    """Processa os arquivos de municípios.

    Args:
        arquivos_munic (list): Lista de arquivos de municípios.
    """
    try:
        logging.info(f"Processar arquivos de municípios")
        for e in range(0, len(arquivos_munic)):
            print('Trabalhando no arquivo: '+arquivos_munic[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_munic[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_munic[e])
            munic = dd.read_csv(extracted_file_path,
                                sep=';',
                                # nrows=100,
                                skiprows=0,
                                header=None,
                                dtype='object',
                                encoding='latin1')
            # Renomear colunas
            munic.columns = column_names

            # Tratamento do arquivo antes de inserir na base:
            munic = munic.reset_index()
            del munic['index']

            # Gravar dados no banco:
            # munic
            for i in range(munic.npartitions):
                df_chunk = munic.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'munic')

            try:
                del munic
            except:
                pass
            print('Arquivos de munic finalizados!')
            munic_insert_end = time.time()
            munic_Tempo_insert = round((munic_insert_end - munic_insert_start))
            print('Tempo de execução do processo de municípios (em segundos): ' +
                str(munic_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de municípios: {e}")
        print(f"Erro ao processar os arquivos de municípios: {e}")
        raise
    processar_arquivos_natju(arquivos['natju'], extracted_files, conexao, cur)
def processar_arquivos_natju(arquivos_natju):
    """Processa os arquivos de natureza jurídica.

    Args:
        arquivos_natju (list): Lista de arquivos de natureza jurídica.
    """
    try:
        logging.info(f"Processar arquivos de natureza jurídica")
        for e in range(0, len(arquivos_natju)):
            print('Trabalhando no arquivo: '+arquivos_natju[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_natju[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_natju[e])
            natju = dd.read_csv(extracted_file_path,
                                sep=';',
                                # nrows=100,
                                skiprows=0,
                                header=None,
                                dtype='object',
                                encoding='latin1')
            # Renomear colunas
            natju.columns = column_names
            # Tratamento do arquivo antes de inserir na base:
            natju = natju.reset_index()
            del natju['index']

            # Gravar dados no banco:
            # natju
            for i in range(natju.npartitions):
                df_chunk = natju.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'natju')

            print('Arquivos de natju finalizados!')
            natju_insert_end = time.time()
            natju_Tempo_insert = round((natju_insert_end - natju_insert_start))
            print('Tempo de execução do processo de natureza jurídica (em segundos): ' +
                str(natju_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de natureza jurídica: {e}")
        print(f"Erro ao processar os arquivos de natureza jurídica: {e}")
        raise
    processar_arquivos_pais(arquivos['pais'], extracted_files, conexao, cur)
def processar_arquivos_pais(arquivos_pais):
    """Processa os arquivos de país.

    Args:
        arquivos_pais (list): Lista de arquivos de país.
    """
    try:
        logging.info(f"Processar arquivos de país")
        for e in range(0, len(arquivos_pais)):
            print('Trabalhando no arquivo: '+arquivos_pais[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_pais[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_pais[e])
            pais = dd.read_csv(extracted_file_path, 
                                sep=';',
                                skiprows=0, 
                                header=None, 
                                dtype='object', 
                                encoding='latin1')
            # Renomear colunas
            pais.columns = column_names
            # Tratamento do arquivo antes de inserir na base:
            pais = pais.reset_index()
            del pais['index']

            # Gravar dados no banco:
            # pais
            for i in range(pais.npartitions):
                df_chunk = pais.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'pais')

            try:
                del pais
            except:
                pass
            print('Arquivos de pais finalizados!')
            pais_insert_end = time.time()
            pais_Tempo_insert = round((pais_insert_end - pais_insert_start))
            print('Tempo de execução do processo de país (em segundos): ' + str(pais_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de país: {e}")
        print(f"Erro ao processar os arquivos de país: {e}")
        raise
def processar_arquivos_qual(arquivos_qual):
    """Processa os arquivos de qualificação de sócios.

    Args:
        arquivos_qual (list): Lista de arquivos de qualificação de sócios.
    """
    try:
        logging.info(f"Processar arquivos de qualificação de sócios")
        for e in range(0, len(arquivos_qual)):
            print('Trabalhando no arquivo: '+arquivos_qual[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_qual[e]} [...]")
            column_data = {name: [] for name in column_names}
            num_particoes = 10
            divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
            # Reparticionar em 10 partições

            extracted_file_path = os.path.join(extracted_files, arquivos_qual[e])
            qual = dd.read_csv(extracted_file_path, 
                                sep=';',
                                skiprows=0, 
                                header=None, 
                                dtype='object', 
                                encoding='latin1')
            # Renomear colunas
            qual.columns = column_names
            # Tratamento do arquivo antes de inserir na base:
            qual = qual.reset_index()
            del qual['index']

            # Gravar dados no banco:
            # qual
            for i in range(qual.npartitions):
                df_chunk = qual.get_partition(i)
                process_and_insert_chunk(df_chunk, conexao,'qual')

            try:
                del qual
            except:
                pass
            print('Arquivos de qual finalizados!')
            qual_insert_end = time.time()
            qual_Tempo_insert = round((qual_insert_end - qual_insert_start))
            print('Tempo de execução do processo de qualificação de sócios (em segundos): ' + str(qual_Tempo_insert))

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de qualificação de sócios: {e}")
        print(f"Erro ao processar os arquivos de qualificação de sócios: {e}")
        raise
def listar_arquivos(diretorio):
    """Lista os arquivos de um diretório.

    Args:
        diretorio (str): Caminho do diretório.

    Returns:
        list: Lista de arquivos.
    """
    try:
        logging.info(f"Listar arquivos")
        return [name for name in os.listdir(diretorio) if os.path.isfile(os.path.join(diretorio, name))]
    except Exception as e:
        logging.error(f"Erro ao listar os arquivos: {e}")
        print(f"Erro ao listar os arquivos: {e}")
        raise

logging.info(f"Iniciando o processo de carga")
# Definir diretórios    
local_env = 'D:\\Repositorio\\00-Programacao\\06-DADOS_RFB\\DADOS_RFB\\code'
load_enviroment(local_env)

# Definir diretórios
output_files, extracted_files = define_directories()

# Acessar o site e obter o conteúdo HTML
dados_rf = 'http://localhost:8000'
raw_html = fetch_data(dados_rf)

# Extrair arquivos do HTML
Files = extract_files_from_html(raw_html)

logging.info(f"Arquivos encontrados: {Files}")
# %%
# Read details from ".env" file:

# Download dos arquivos
download_files(Files, dados_rf, output_files)

# Descompactação dos arquivos
extract_files(Files, output_files, extracted_files)

# Listar os arquivos no diretório
items = listar_arquivos(extracted_files)

# Iniciar o processo de leitura e inserção dos dados
insert_start = time.time()
logging.info(f"LER E INSERIR DADOS")

# Separar os arquivos em listas de acordo com o nome
logging.info(f"Separar arquivos")
arquivos = separar_arquivos(items)

# Acesso ao banco de dados
logging.info("Acesso ao banco de dados")
conexao, cur = connect_to_database()

if conexao is None or cur is None:
    logging.info("Conexão falhou")
else:
    # Processar arquivos de empresa
    processar_arquivos_empresa(arquivos['empresa'], extracted_files, conexao, cur)
    processar_arquivos_estabelecimento(arquivos['estabelecimento'], extracted_files, conexao, cur)
    processar_arquivos_socios(arquivos['socios'], extracted_files, conexao, cur)
    processar_arquivos_simples(arquivos['simples'], extracted_files, conexao, cur)
    processar_arquivos_cnae(arquivos['cnae'], extracted_files, conexao, cur)
    processar_arquivos_moti(arquivos['moti'], extracted_files, conexao, cur)
    processar_arquivos_munic(arquivos['munic'], extracted_files, conexao, cur)
    processar_arquivos_natju(arquivos['natju'], extracted_files, conexao, cur)
    processar_arquivos_pais(arquivos['pais'], extracted_files, conexao, cur)
    processar_arquivos_qual(arquivos['quals'], extracted_files, conexao, cur)
    logging.info(f"Processo de carga dos arquivos finalizado")
    
    insert_end = time.time()
    Tempo_insert = round(insert_end - insert_start)
    print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert))
    
    index_start = time.time()
    
    # Criação de índices
    cur.execute('CREATE INDEX empresa_cnpj ON empresa(cnpj_basico);')
    cur.execute('conexao.commit;')
    cur.execute('CREATE INDEX estabelecimento_cnpj ON estabelecimento(cnpj_basico);')
    cur.execute('conexao.commit;')
    cur.execute('CREATE INDEX socios_cnpj ON socios(cnpj_basico);')
    cur.execute('conexao.commit;')
    cur.execute('CREATE INDEX simples_cnpj ON simples(cnpj_basico);')
    cur.execute('conexao.commit;')
    conexao.commit()
    
    index_end = time.time()
    index_time = round(index_end - index_start)
    print('Tempo para criar os índices (em segundos): ' + str(index_time))
    # %%
    print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
     - Desenvolvido por: Aphonso Henrique do Amaral Rafael
     - Adaptado por: Vander Ribeiro Elme
    - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
    """)
