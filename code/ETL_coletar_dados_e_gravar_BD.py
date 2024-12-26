from dotenv import load_dotenv
import bs4 as bs
import csv
import dask.dataframe as dd
import getenv
import hashlib
import logging
import lxml
import mysql.connector
from mysql.connector.errors import OperationalError
import numpy as np
import os
import pandas as pd
import re
import requests
import sys
import time
import urllib.parse
import urllib.request
import wget
import zipfile

# Configuração do logging
logging.basicConfig(filename='DADOS_RFB.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    cnpj_basico = ''
    current = 1
    i = 0
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
        logging.info(
            f"Verificar diferença entre o arquivo local e o arquivo remoto")
        response = requests.head(url)
        response.raise_for_status()

        remote_size = int(response.headers.get('content-length', 0))
        if os.path.exists(file_name):
            with open(file_name, 'rb') as f:
                local_hash = hashlib.sha256(f.read()).hexdigest()

            # Verificar se o arquivo remoto é diferente do arquivo local
            if os.path.getsize(file_name) != remote_size:
                logging.info(f"O arquivo remoto: {
                             remote_size} é diferente do arquivo local")
                return True

            # Verificar se o hash do arquivo remoto é diferente do arquivo local
            response = requests.get(url)
            response.raise_for_status()
            remote_hash = hashlib.sha256(response.content).hexdigest()
            if local_hash != remote_hash:
                logging.info(
                    f"O hash do arquivo remoto é diferente do arquivo local")
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

        logging.info(f"Diretórios definidos: output_files: {
                     output_files}, extracted_files: {extracted_files}")
        print('Diretórios definidos: \n' + 'output_files: ' +
              str(output_files) + '\n' + 'extracted_files: ' + str(extracted_files))
        return output_files, extracted_files
    except OSError as e:
        logging.error(
            f"Erro na definição dos diretórios, verifique o arquivo '.env' ou o local informado do seu arquivo de configuração: {e}")
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
    messagem = "Executando: %d%% [%d / %d] bytes - " % (
        current / total * 100, current, total)
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


def connect_to_database(max_retries=3, delay=5):
    """Conecta ao banco de dados com tentativas de reconexão.

    Args:
        max_retries (int): Número máximo de tentativas de reconexão.
        delay (int): Tempo de espera entre as tentativas de reconexão.

    Returns:
        mysql.connector.connection.MySQLConnection: Conexão com o banco de dados.
    """
    for attempt in range(max_retries):
        try:
            logging.info(f"Tentativa de conexão ao banco de dados ({
                         attempt + 1}/{max_retries})")
            conexao = mysql.connector.connect(
                host=os.getenv('db_host'),
                user=os.getenv('db_user'),
                password=os.getenv('db_password'),
                database=os.getenv('db_name'),
                use_pure=True
            )
            logging.info(
                "Conexão com o banco de dados estabelecida com sucesso")
            return conexao
        except mysql.connector.Error as e:
            logging.error(f"Erro ao conectar ao banco de dados: {e}")
            print(f"Erro ao conectar ao banco de dados: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Tentando reconectar ao banco de dados em {
                             delay} segundos...")
                time.sleep(delay)
            else:
                raise


def ensure_connection(conexao, max_retries=3, delay=5):
    """Garante que a conexão com o banco de dados esteja ativa, tentando reconectar se necessário.

    Args:
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
        max_retries (int): Número máximo de tentativas de reconexão.
        delay (int): Tempo de espera entre as tentativas de reconexão.
    """
    for attempt in range(max_retries):
        try:
            conexao.ping(reconnect=True, attempts=3, delay=5)
            return
        except mysql.connector.Error as e:
            logging.error(
                f"Erro ao verificar a conexão com o banco de dados: {e}")
            print(f"Erro ao verificar a conexão com o banco de dados: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Tentando reconectar ao banco de dados em {
                             delay} segundos...")
                time.sleep(delay)
            else:
                raise


def duracao_processo(start_time, end_time):
    """Calcula a duração do processo.

    Args:
        start_time (float): Tempo de início do processo.
        end_time (float): Tempo de fim do processo.

    Returns:
        int: Duração do processo em segundos.
    """
    try:
        logging.info(f'Duração do processo: {end_time - start_time}')
        return
    except Exception as e:
        logging.error(f"Erro ao calcular a duração do processo: {e}")
        print(f"Erro ao calcular a duração do processo: {e}")
        raise


def process_and_insert_chunk(df_chunk, conexao, table_name, table_schema):
    """
        Grava dados no banco de dados.

        Args:
        df_chunk (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
        table_name (str): Nome da tabela no banco de dados.
        table_schema (str): Esquema SQL para criar a tabela, se necessário.
    """
    logging.info(f"Gravar dados no banco {table_name}")
    cursor = None
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ensure_connection(conexao)
            cursor = conexao.cursor()
            # Verificar se a tabela existe
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            if not result:
                logging.info(
                    f"Tabela {table_name} não existe. Criando tabela.")
                cursor.execute(table_schema)
                conexao.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")

            # Processo para inserir os dados no banco
            logging.info(
                f"Processo para inserir os dados no banco {table_name}")
            # Corrigir os valores decimais
            df_chunk['capital_social'] = df_chunk['capital_social'].str.replace(
                ',', '.')
            # Converter o DataFrame para uma lista de tuplas
            data_tuples = [tuple(x) for x in df_chunk.to_numpy()]
            # Inserção em massa
            columns = ', '.join(df_chunk.columns)
            placeholders = ', '.join(['%s'] * len(df_chunk.columns))
            insert_query = f"INSERT INTO {
                table_name} ({columns}) VALUES ({placeholders})"
            cursor.executemany(insert_query, data_tuples)
            conexao.commit()
            logging.info(
                f'Dados inseridos com sucesso na tabela: {table_name}')
            break
        except OperationalError as e:
            logging.error(f"Erro operacional ao inserir dados na tabela {
                          table_name}: {e}")
            print(f"Erro operacional ao inserir dados na tabela {
                  table_name}: {e}")
            if attempt < max_retries - 1:
                logging.info("Tentando reconectar ao banco de dados...")
                try:
                    conexao.reconnect(attempts=3, delay=5)
                    logging.info("Reconexão bem-sucedida.")
                except mysql.connector.Error as reconnection_error:
                    logging.error(f"Erro ao reconectar ao banco de dados: {
                                  reconnection_error}")
                    print(f"Erro ao reconectar ao banco de dados: {
                          reconnection_error}")
            else:
                logging.error(f"Falha ao inserir dados na tabela {
                              table_name} após {max_retries} tentativas")
                raise
        except mysql.connector.Error as e:
            logging.error(f"Erro ao inserir dados na tabela {table_name}: {e}")
            print(f"Erro ao inserir dados na tabela {table_name}: {e}")
            if attempt < max_retries - 1:
                logging.info("Tentando reconectar ao banco de dados...")
                try:
                    conexao.reconnect(attempts=3, delay=5)
                    logging.info("Reconexão bem-sucedida.")
                except mysql.connector.Error as reconnection_error:
                    logging.error(f"Erro ao reconectar ao banco de dados: {
                                  reconnection_error}")
                    print(f"Erro ao reconectar ao banco de dados: {
                          reconnection_error}")
            else:
                logging.error(f"Falha ao inserir dados na tabela {
                              table_name} após {max_retries} tentativas")
                raise
        finally:
            if cursor is not None:
                cursor.close()
            logging.info(
                f"Finalizando processo de inserção de dados na tabela {table_name}")


def processar_arquivos(arquivos, extracted_files, conexao, table_name, table_schema, column_names):
    """Processa os arquivos e insere os dados no banco de dados.

    Args:
        arquivos (list): Lista de arquivos a serem processados.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
        table_name (str): Nome da tabela no banco de dados.
        table_schema (str): Esquema SQL para criar a tabela, se necessário.
        column_names (list): Lista de nomes das colunas.
    """
    try:
        start_time = time.time()
        logging.info(f"Processar arquivos de {table_name}")
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute(f'DROP TABLE IF EXISTS {table_name};')
        conexao.commit()
        for e in range(0, len(arquivos)):
            print(f'Trabalhando no arquivo: {arquivos[e]} [...]')
            logging.info(f'Trabalhando no arquivo: {arquivos[e]} [...]')
            extracted_file_path = os.path.join(extracted_files, arquivos[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f, delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)

                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, table_name, table_schema)

            end_time = time.time()
            duracao_processo(start_time, end_time)
    except mysql.connector.Error as e:
        logging.error(f"Erro ao processar os arquivos de {table_name}: {e}")
        print(f"Erro ao processar os arquivos de {table_name}: {e}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        logging.info(
            f"Finalizando processo de inserção de dados na tabela {table_name}")


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
conexao = connect_to_database()

if conexao is None:
    logging.info("Conexão falhou")
else:
    tabelas = {
        'empresa': {
            'schema': """
                CREATE TABLE empresa (
                    cnpj_basico VARCHAR(14),
                    razao_social VARCHAR(255),
                    natureza_juridica VARCHAR(255),
                    qualificacao_responsavel VARCHAR(255),
                    capital_social DECIMAL(15, 2),
                    porte_empresa VARCHAR(255),
                    ente_federativo_responsavel VARCHAR(255)
                )
            """,
            'columns': ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
        },
        'estabelecimento': {
            'schema': """
                CREATE TABLE estabelecimento (
                    cnpj_basico VARCHAR(8),
                    cnpj_ordem VARCHAR(4),
                    cnpj_dv VARCHAR(2),
                    identificador_matriz_filial VARCHAR(1),
                    situacao_cadastral VARCHAR(2),
                    data_situacao VARCHAR(8),
                    motivo_situacao_cadastral VARCHAR(2),
                    nome_cidade_exterior VARCHAR(55),
                    codigo_pais VARCHAR(5),
                    data_inicio_atividade VARCHAR(8),
                    cnae_fiscal_principal VARCHAR(7),
                    cnae_fiscal_secundaria VARCHAR(7),
                    tipo_logradouro VARCHAR(20),
                    logradouro VARCHAR(60),
                    numero VARCHAR(6),
                    complemento VARCHAR(156),
                    bairro VARCHAR(50),
                    cep VARCHAR(8),
                    uf VARCHAR(2),
                    municipio VARCHAR(50),
                    ddd_1 VARCHAR(4),
                    telefone_1 VARCHAR(12),
                    ddd_2 VARCHAR(4),
                    telefone_2 VARCHAR(12),
                    dd_fax VARCHAR(4),
                    fax VARCHAR(12),
                    correio_eletronico VARCHAR(115),
                    situacao_especial VARCHAR(23),
                    data_situacao_especial VARCHAR(8)
                )
            """,
            'columns': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'situacao_cadastral', 'data_situacao', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'codigo_pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'dd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
        },
        # ...definir schemas e colunas para as outras tabelas...
    }

    for tabela, info in tabelas.items():
        processar_arquivos(arquivos[tabela], extracted_files,
                           conexao, tabela, info['schema'], info['columns'])

    logging.info(f"Processo de carga dos arquivos finalizado")

    insert_end = time.time()
    Tempo_insert = round(insert_end - insert_start)
    print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert))

    index_start = time.time()

    # Criação de índices
    cursor = conexao.cursor()
    cursor.execute('CREATE INDEX empresa_cnpj ON empresa(cnpj_basico);')
    cursor.execute('conexao.commit;')
    cursor.execute(
        'CREATE INDEX estabelecimento_cnpj ON estabelecimento(cnpj_basico);')
    cursor.execute('conexao.commit;')
    cursor.execute('CREATE INDEX socios_cnpj ON socios(cnpj_basico);')
    cursor.execute('conexao.commit;')
    cursor.execute('CREATE INDEX simples_cnpj ON simples(cnpj_basico);')
    cursor.execute('conexao.commit;')
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
