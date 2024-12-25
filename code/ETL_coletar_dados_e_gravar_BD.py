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
        logging.info("Conexão com o banco de dados estabelecida com sucesso")
        return conexao
    except mysql.connector.Error as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        print(f"Erro ao conectar ao banco de dados: {e}")
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
def processar_arquivos_empresa(arquivos_empresa, extracted_files, conexao):
    """Processa os arquivos de empresa.

    Args:
        arquivos_empresa (list): Lista de arquivos de empresa.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'empresa'
        table_schema = """
        CREATE TABLE empresa (
            cnpj_basico VARCHAR(14),
            razao_social VARCHAR(255),
            natureza_juridica VARCHAR(255),
            qualificacao_responsavel VARCHAR(255),
            capital_social DECIMAL(15, 2),
            porte_empresa VARCHAR(255),
            ente_federativo_responsavel VARCHAR(255)
        )
        """
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS empresa;')
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

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'empresa', table_schema)

            end_time = time.time()
            duracao_processo(start_time, end_time)
    except mysql.connector.Error as e:
        logging.error(f"Erro ao processar os arquivos de empresa: {e}")
        print(f"Erro ao processar os arquivos de empresa: {e}")
        raise
def processar_arquivos_estabelecimento(arquivos_estabelecimento, extracted_files, conexao):
    """Processa os arquivos de estabelecimento.

    Args:
        arquivos_estabelecimento (list): Lista de arquivos de estabelecimento.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """

    try:
        start_time = time.time()
        table_name = 'estabelecimento'
        logging.info(f'Processar arquivos de {table_name}')
        table_schema = """
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
        """        
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS estabelecimento;')
        conexao.commit()
        column_names = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
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

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'estabelecimento', table_schema)

            end_time = time.time()
            duracao_processo(start_time, end_time)

    except mysql.connector.Error as e:
        logging.error(f"Erro ao processar a tabela {table_name}: {e}")
        
        # Tentar reconectar ao banco de dados
    finally:
        if cursor is not None:
            cursor.close()
        logging.info(f"Finalizando processo de inserção de dados na tabela {table_name}")
def processar_arquivos_socios(arquivos_socios, extracted_files, conexao):
    """Processa os arquivos de sócios.

    Args:
        arquivos_socios (list): Lista de arquivos de sócios.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'socios'
        logging.info(f"Processar arquivos de {table_name}")
        table_schema = """
        CREATE TABLE socios (
            cnpj_basico VARCHAR(14),
            identificador_socio VARCHAR(1),
            nome_socio VARCHAR(60),
            cnpj_cpf_socio VARCHAR(14),
            qualificacao_socio VARCHAR(2),
            data_entrada_sociedade VARCHAR(8),
            pais VARCHAR(50),
            representante_legal VARCHAR(1),
            nome_representante_legal VARCHAR(60),
            qualificacao_representante_legal VARCHAR(2),
            faixa_etaria VARCHAR(1)
        )
        """
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS socios;')
        conexao.commit()
        column_names = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
        for e in range(0, len(arquivos_socios)):
            print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_socios[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'socios', table_schema)

            end_time = time.time()
            duracao_processo(start_time, end_time)
        
    except mysql.connector.Error as e:
        logging.error(f"Erro ao processar os arquivos de {table_name}: {e}")
        print(f"Erro ao processar os arquivos de {table_name}: {e}")
        raise
def processar_arquivos_simples(arquivos_simples, extracted_files, conexao):
    """Processa os arquivos de Simples Nacional.

    Args:
        arquivos_simples (list): Lista de arquivos de Simples Nacional.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'simples'
        logging.info(f"Processar arquivos de {table_name}")
        table_schema = """
        CREATE TABLE simples (
            cnpj_basico VARCHAR(8),
            opcao_simples VARCHAR(1),
            data_opcao_simples VARCHAR(8),
            data_exclusao_simples VARCHAR(8),
            opcao_mei VARCHAR(1),
            data_opcao_mei VARCHAR(8),
            data_exclusao_mei VARCHAR(8)
        )
        """
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS simples;')
        conexao.commit()
        column_names = ['cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
        for e in range(0, len(arquivos_simples)):
            print('Trabalhando no arquivo: '+arquivos_simples[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_simples[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'simples', table_schema)
      
            end_time = time.time()
            duracao_processo(start_time, end_time)

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de {table_name}: {e}")
        print(f"Erro ao processar os arquivos de Simples {table_name}: {e}")
def processar_arquivos_cnae(arquivos_cnae, extracted_files, conexao):
    """Processa os arquivos de CNAE.

    Args:
        arquivos_cnae (list): Lista de arquivos de CNAE.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'cnae'
        table_schema = """
        CREATE TABLE cnae (
            codigo_cnae VARCHAR(7),
            descricao_cnae VARCHAR(255)
            )
        """
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS cnae;')
        conexao.commit()
        column_names = ['codigo_cnae', 'descricao_cnae']
        logging.info(f"Processar arquivos de {table_name}")
        for e in range(0, len(arquivos_cnae)):
            print('Trabalhando no arquivo: '+arquivos_cnae[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_cnae[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_cnae[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'cnae', table_schema)
            
            end_time = time.time()
            duracao_processo(start_time, end_time)
    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de {table_name}: {e}")
        print(f"Erro ao processar os arquivos de {table_name}: {e}")
        raise
def processar_arquivos_moti(arquivos_moti, extracted_files, conexao):
    """Processa os arquivos de motivos da situação atual.

    Args:
        arquivos_moti (list): Lista de arquivos de motivos da situação atual.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'moti'
        table_schema = """
        CREATE TABLE moti (
            motivo_situacao_cadastral VARCHAR(2),
            descricao_motivo VARCHAR(255)
            )
        """
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS moti;')
        conexao.commit()
        column_names = ['motivo_situacao_cadastral', 'descricao_motivo']
        for e in range(0, len(arquivos_moti)):
            print('Trabalhando no arquivo: '+arquivos_moti[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_moti[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_moti[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'moti', table_schema)
            
            end_time = time.time()
            duracao_processo(start_time, end_time)      

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de motivos da situação atual: {e}")
        print(f"Erro ao processar os arquivos de motivos da situação atual: {e}")
        raise
def processar_arquivos_munic(arquivos_munic, extracted_files, conexao):
    """Processa os arquivos de municípios.

    Args:
        arquivos_munic (list): Lista de arquivos de municípios.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'munic'
        table_schema = """
        CREATE TABLE munic (
            codigo_municipio VARCHAR(7),
            descricao_municipio VARCHAR(255)
            )
        """            

        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS munic;')
        conexao.commit()
        column_names = ['codigo_municipio', 'descricao_municipio']
        for e in range(0, len(arquivos_munic)):
            print('Trabalhando no arquivo: '+arquivos_munic[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_munic[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_munic[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'munic', table_schema)
            
            end_time = time.time()
            duracao_processo(start_time, end_time)

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de municípios: {e}")
        print(f"Erro ao processar os arquivos de municípios: {e}")
        raise
def processar_arquivos_natju(arquivos_natju, extracted_files, conexao):
    """Processa os arquivos de natureza jurídica.

    Args:
        arquivos_natju (list): Lista de arquivos de natureza jurídica.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'natju'
        table_schema = """
        CREATE TABLE natju (
            codigo_natureza_juridica VARCHAR(3),
            descricao_natureza_juridica VARCHAR(255)
            )
        """
       
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS natju;')
        conexao.commit()
        column_names = ['codigo_natureza_juridica', 'descricao_natureza_juridica']
        for e in range(0, len(arquivos_natju)):
            print('Trabalhando no arquivo: '+arquivos_natju[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_natju[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_natju[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'natju', table_schema)
            
            end_time = time.time()
            duracao_processo(start_time, end_time)

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de natureza jurídica: {e}")
        print(f"Erro ao processar os arquivos de natureza jurídica: {e}")
        raise
def processar_arquivos_pais(arquivos_pais, extracted_files, conexao):
    """Processa os arquivos de país.

    Args:
        arquivos_pais (list): Lista de arquivos de país.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'pais'
        table_schema = """
        CREATE TABLE pais (
            codigo_pais VARCHAR(5),
            descricao_pais VARCHAR(50)
            )
            """
       
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS pais;')
        conexao.commit()
        column_names = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
        logging.info(f"Processar arquivos de {table_name}")
        for e in range(0, len(arquivos_pais)):
            print('Trabalhando no arquivo: '+arquivos_pais[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_pais[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_pais[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'pais', table_schema)
            
            end_time = time.time()
            duracao_processo(start_time, end_time)

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de país: {e}")
        print(f"Erro ao processar os arquivos de país: {e}")
        raise
def processar_arquivos_qual(arquivos_qual, extracted_files, conexao):
    """Processa os arquivos de qualificação de sócios.

    Args:
        arquivos_qual (list): Lista de arquivos de qualificação de sócios.
        extracted_files (str): Caminho do diretório onde os arquivos foram extraídos.
        conexao (mysql.connector.connection.MySQLConnection): Conexão com o banco de dados.
    """
    try:
        start_time = time.time()
        table_name = 'qual'
        table_schema = """
        create table qual (
            codigo_pais varchar(5),
            descricao_pais varchar(50)
            )
        """       
        num_particoes = 10
        cursor = conexao.cursor()
        # Drop table antes do insert
        cursor.execute('DROP TABLE IF EXISTS qual;')
        conexao.commit()
        column_names = ['codigo_pais', 'descricao_pais']
        logging.info(f"Processar arquivos de qualificação de sócios")
        for e in range(0, len(arquivos_qual)):
            print('Trabalhando no arquivo: '+arquivos_qual[e]+' [...]')
            logging.info(f"Trabalhando no arquivo: {arquivos_qual[e]} [...]")
            extracted_file_path = os.path.join(extracted_files, arquivos_qual[e])
            column_data = {name: [] for name in column_names}
            with open(extracted_file_path, 'r', encoding='latin1') as f:
                reader = csv.reader(f,delimiter=';')
                for row in reader:
                    for i, col in enumerate(column_names):
                        column_data[col].append(row[i])

                df = pd.DataFrame(column_data)
                
                # Insere o DataFrame no banco de dados
                process_and_insert_chunk(df, conexao, 'qual', table_schema)
            
            end_time = time.time()
            duracao_processo(start_time, end_time)

    except Exception as e:
        logging.error(f"Erro ao processar os arquivos de qualificação de sócios: {e}")
        print(f"Erro ao processar os arquivos de qualificação de sócios: {e}")
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
            cursor = conexao.cursor()
            # Verificar se a tabela existe
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            if not result:
                logging.info(f"Tabela {table_name} não existe. Criando tabela.")
                cursor.execute(table_schema)
                conexao.commit()
                logging.info(f"Tabela {table_name} criada com sucesso")

            # Processo para inserir os dados no banco
            logging.info(f"Processo para inserir os dados no banco {table_name}")
            # Corrigir os valores decimais
            df_chunk['capital_social'] = df_chunk['capital_social'].str.replace(',', '.')
            # Converter o DataFrame para uma lista de tuplas
            data_tuples = [tuple(x) for x in df_chunk.to_numpy()]
            # Inserção em massa
            columns = ', '.join(df_chunk.columns)
            placeholders = ', '.join(['%s'] * len(df_chunk.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.executemany(insert_query, data_tuples)
            conexao.commit()
            logging.info(f'Dados inseridos com sucesso na tabela: {table_name}')
            break

        except mysql.connector.Error as e:
            logging.error(f"Erro ao inserir dados na tabela {table_name}: {e}")
            print(f"Erro ao inserir dados na tabela {table_name}: {e}")
            if attempt < max_retries - 1:
                logging.info("Tentando reconectar ao banco de dados...")
                try:
                    conexao.reconnect(attempts=3, delay=5)
                    logging.info("Reconexão bem-sucedida.")
                except mysql.connector.Error as reconnection_error:
                    logging.error(f"Erro ao reconectar ao banco de dados: {reconnection_error}")
                    print(f"Erro ao reconectar ao banco de dados: {reconnection_error}")
            else:
                logging.error(f"Falha ao inserir dados na tabela {table_name} após {max_retries} tentativas")
                raise
        finally:
            if cursor is not None:
                cursor.close()
            logging.info(f"Finalizando processo de inserção de dados na tabela {table_name}")

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
    # Processar arquivos de empresa
    processar_arquivos_empresa(arquivos['empresa'], extracted_files, conexao)
    processar_arquivos_estabelecimento(arquivos['estabelecimento'], extracted_files, conexao)
    processar_arquivos_socios(arquivos['socios'], extracted_files, conexao)
    processar_arquivos_simples(arquivos['simples'], extracted_files, conexao)
    processar_arquivos_cnae(arquivos['cnae'], extracted_files, conexao)
    processar_arquivos_moti(arquivos['moti'], extracted_files, conexao)
    processar_arquivos_munic(arquivos['munic'], extracted_files, conexao)
    processar_arquivos_natju(arquivos['natju'], extracted_files, conexao)
    processar_arquivos_pais(arquivos['pais'], extracted_files, conexao)
    processar_arquivos_qual(arquivos['quals'], extracted_files, conexao)
    logging.info(f"Processo de carga dos arquivos finalizado")
    
    insert_end = time.time()
    Tempo_insert = round(insert_end - insert_start)
    print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert))
    
    index_start = time.time()
    
    # Criação de índices
    cursor = conexao.cursor()
    cursor.execute('CREATE INDEX empresa_cnpj ON empresa(cnpj_basico);')
    cursor.execute('conexao.commit;')
    cursor.execute('CREATE INDEX estabelecimento_cnpj ON estabelecimento(cnpj_basico);')
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
