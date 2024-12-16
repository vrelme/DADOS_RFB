#from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine
#from sqlalchemy.ext.declarative import declarative_base
import bs4 as bs
import dask.dataframe as dd
#import ftplib
import getenv
import hashlib
import logging
#import lxml
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
#import threading
import urllib.parse
import urllib.request
import wget
import zipfile

cnpj_basico=''
current = 1
i=0
# Gerar Log
logging.basicConfig(filename='DADOS_RFB.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
        response = requests.head(url)
        response.raise_for_status()
    
        remote_size = int(response.headers.get('content-length', 0))
        if os.path.exists(file_name):
            with open(file_name, 'rb') as f:
                local_hash = hashlib.sha256(f.read()).hexdigest()
            response = requests.get(url, stream=True)
            hasher = hashlib.sha256()
            for chunk in response.iter_content(chunk_size=8192):  
                hasher.update(chunk)
            remote_hash = hasher.hexdigest()
            return local_hash != remote_hash
        else:
            return True
    except requests.exceptions.RequestException as e:
        print(f"Erro ao verificar o arquivo remoto: {e}")
        logging.info(f"Erro ao verificar o arquivo remoto")
        return True
    except OSError as e:
        print(f"Erro ao acessar o arquivo local: {e}")
        logging.info(f"Erro ao acessar o arquivo local")
        return True

# %%
def makedirs_custom(path, exist_ok=True, mode=0o755):
    """Cria diretórios recursivamente com controle de existência e permissões.

    Args:
        path (str): Caminho completo do diretório a ser criado.
        exist_ok (bool, opcional): Se True, não gera erro se o diretório já existe.
        mode (int, opcional): Permissões do diretório.

    Returns:
        bool: True se o diretório foi criado com sucesso, False caso contrário.
    """
    try:
        os.makedirs_custom(path, exist_ok=exist_ok, mode=mode)
        return True
    except OSError as e:
        logging.error(f"Erro ao criar diretório {path}: {e}")
        return False


# %%

def to_sql(dataframe, table_name, connection_uri, if_exists='append', index=False, chunksize=10000):
    """Insere um DataFrame em uma tabela de banco de dados.

    Args:
        dataframe (pd.DataFrame): DataFrame a ser inserido.
        table_name (str): Nome da tabela no banco de dados.
        connection_uri (str): URI de conexão com o banco de dados.
        if_exists (str, optional): Ação a ser tomada se a tabela já existir. Defaults to 'append'.
        index (bool, optional): Se True, inclui o índice do DataFrame na tabela. Defaults to False.
        chunksize (int, optional): Tamanho dos chunks para inserção. Defaults to 10000.
    """

    try:
        for chunk in chunker(dataframe, chunksize):
            chunk.to_sql(table_name, connection_uri, if_exists='append', index=index, chunksize=10000)
    except Exception as e:
        logging.error(f"Erro ao inserir dados: {e}")
        print(f"Erro ao inserir dados: {e}")
    

def chunker(df, chunksize=10000):
    """Divide um DataFrame em chunks de tamanho especificado.

    Args:
        df (pd.DataFrame): DataFrame a ser dividido em chunks.
        chunksize (int, optional): Tamanho de cada chunk. Defaults to 10000.

    Yields:
        pd.DataFrame: Próximo chunk do DataFrame.
    """
    try:
        return (df[i:i + size] for i in range(0, len(df), size))
        print(chunker)
    
        for df_chunk in (dataframe[i:i + size] for i in range(0, len(dataframe),size)):
            df_chunk.to_sql(**kwargs)
    except Exception as e:
        logging.error(f"Erro na thread: {e}")
    finally:
        # Limpar recursos
        logging.info(f"Thread")        

    # Dividir o DataFrame em chunks de 10.000 linhas
    for chunk in chunker(dataframe, chunksize=10000):
        # Processar cada chunk (por exemplo, inserir em um banco de dados)
        print(chunk.head())

# Gravar dados no banco:
# Empresa
    
def process_and_insert_chunk(df_chunk, conexao, table_name):
    # Conexao (URI)
    try:
        connection_uri = f"mysql+mysqlconnector://{os.getenv('db_user')}:{os.getenv('db_password')}@{os.getenv('db_host')}:{os.getenv('DB_PORT')}/{os.getenv('db_name')}"
        # Processo para inserir no banco de dados
        df_chunk.to_sql(table_name, uri, if_exists='append', index=False)
        logging.info(f"Tabela: {table_name} inserido com sucesso no banco de dados!")
        bar_progress(current, 37)
        current=+1
        pass
    except Exception as e:
        logging.error(f"Erro na thread: {e}")
    finally:
        logging.info(f"Thread")    

# %%
# Ler arquivo de configuração de ambiente # https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1

def getEnv(env):
    return os.getenv(env)

# print('Especifique o local do seu arquivo de configuração ".env". Por exemplo: C:\...\Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ\code')
# C:\Aphonso_C\Git\Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ\code
local_env = 'D:\\Repositorio\\00_Programação\\06 - DADOS_RFB\\DADOS_RFB\\code'
dotenv_path = os.path.join(local_env, '.env')
load_dotenv(dotenv_path=dotenv_path)

dados_rf = 'http://localhost:8000'

logging.info(f"Acesso ao site")
# %%
# Read details from ".env" file:
output_files = None
extracted_files = None
try:
    output_files = getEnv('OUTPUT_FILES_PATH')
    makedirs_custom(output_files, mode=0o755)

    extracted_files = getEnv('EXTRACTED_FILES_PATH')
    makedirs_custom(extracted_files, mode=0o755)

    print('Diretórios definidos: \n' +
          'output_files: ' + str(output_files) + '\n' +
          'extracted_files: ' + str(extracted_files))
except Exception as e:
    logging.error(f"Erro na definição dos diretórios, verifique o arquivo '.env' ou o local informado do seu arquivo de configuração. {dotenv_path}: {e}")
    pass
    
# %%
raw_html = urllib.request.urlopen(dados_rf)
raw_html = raw_html.read()

# Formatar página e converter em string
page_items = bs.BeautifulSoup(raw_html, 'lxml')
html_str = str(page_items)

# Obter arquivos
Files = []
text = '.zip'
for m in re.finditer(text, html_str):  # type: ignore
    i_start = m.start()-40
    i_end = m.end()
    i_loc = html_str[i_start:i_end].find('href=')+6  # type: ignore
    Files.append(html_str[i_start+i_loc:i_end])  # type: ignore

# Correcao do nome dos arquivos devido a mudanca na estrutura do HTML da pagina - 31/07/22 - Aphonso Rafael
Files_clean = []
for i in range(len(Files)):
    if not Files[i].find('.zip">') > -1:
        Files_clean.append(Files[i])

try:
    del Files
except:
    pass

Files = Files_clean

print('Arquivos que serão baixados:')
i_f = 0
for f in Files:
    i_f += 1
    print(str(i_f) + ' - ' + f)

# %%
########################################################################################################################
## DOWNLOAD ############################################################################################################
########################################################################################################################
# Create this bar_progress method which is invoked automatically from wget:

def bar_progress(current, total, width=80):
    messagem = "Executando: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
    # Don't use print() as it will print in new line every time.
    sys.stdout.write("\r" + messagem)
    sys.stdout.flush()

# %%
# Inicio Download dos arquivos ################################################################
logging.info(f"Download arquivos")
print('Baixando arquivo:')
i_l = 0
for l in Files:
    # Download dos arquivos
    i_l += 1
    print(str(i_l) + ' - ' + l)
    url = dados_rf+'/'+l  # type: ignore
    file_name = os.path.join(output_files, l)
    if check_diff(url, file_name):
        wget.download(url, out=output_files, bar=bar_progress)
logging.info(f"Fim do Download doa arquivos")
# Fim do Download doa arquivos ######################################################
# %%
# Descompactando arquivo ####################################################################################################################################################
print('Descompactando arquivo:')
i_l = 0
logging.info(f"Descompactando arquivo")
for l in Files:
    try:
        i_l += 1
        print(str(i_l) + ' - ' + l)
        full_path = os.path.join(output_files, l)
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
    except:
        pass
logging.info(f"Fim descompactando arquivo")
# Descompactando arquivo ####################################################################################################################################################
 
# %%
########################################################################################################################
## LER E INSERIR DADOS #################################################################################################
########################################################################################################################
insert_start = time.time()
logging.info(f"LER E INSERIR DADOS")
# Files:
Items = [name for name in os.listdir(extracted_files) if name.endswith('')]

# Separar arquivos:
logging.info('Separar arquivos')
arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []
for i in range(len(Items)):
    if Items[i].find('EMPRE') > -1:
        arquivos_empresa.append(Items[i])
    elif Items[i].find('ESTABELE') > -1:
        arquivos_estabelecimento.append(Items[i])
    elif Items[i].find('SOCIO') > -1:
        arquivos_socios.append(Items[i])
    elif Items[i].find('SIMPLES') > -1:
        arquivos_simples.append(Items[i])
    elif Items[i].find('CNAE') > -1:
        arquivos_cnae.append(Items[i])
    elif Items[i].find('MOTI') > -1:
        arquivos_moti.append(Items[i])
    elif Items[i].find('MUNIC') > -1:
        arquivos_munic.append(Items[i])
    elif Items[i].find('NATJU') > -1:
        arquivos_natju.append(Items[i])
    elif Items[i].find('PAIS') > -1:
        arquivos_pais.append(Items[i])
    elif Items[i].find('QUALS') > -1:
        arquivos_quals.append(Items[i])
    else:
        pass

# %%
# Conectar no banco de dados:
# Dados da conexão com o BD
logging.info(f"Acesso Banco de dados")
try:
    conexao = mysql.connector.connect(
                host=os.getenv('db_host'),
                user=os.getenv('db_user'),
                password=os.getenv('db_password'),
                database=os.getenv('db_name')   
                )

    cur = conexao.cursor()

except mysql.connector.Error as err:
    print(f"Erro na thread: {e}")
    logging.info(f"Conexao falhou")

# Criação e inicialização da thread
thread = threading.Thread(target=my_thread_function)
thread.start()

# %%
# Arquivos de empresa:
empresa_insert_start = time.time()
print("""
#########################
## Arquivos de EMPRESA:  ##
#########################
""")
i=0
logging.info(f"Ler arquivos de Empresa")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS empresa;')
conexao.commit()
for e in range(0, len(arquivos_empresa)):
    print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
    column_names = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    empresa = dd.DataFrame(column_data, name='empresa_dataframe', meta={'cnpj_basico': 'object', 'razao_social': 'object', 'natureza_juridica': 'object', 'qualificacao_responsavel': 'object', '': 'object', 'porte_empresa': 'object', 'ente_federativo_responsavel': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições
 
    extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])
    empresa = dd.read_csv(extracted_file_path,
                          sep=';',
                          # nrows=100,
                          skiprows=0,
                          header=None,
                          dtype='object',
                          encoding='latin1')
    # Renomear colunas                     
    empresa.columns = column_names
    # Replace "," por "."
    if 'capital_social' in empresa.columns:
        # Apply the transformation if the column exists
        empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',', '.'))
        empresa['capital_social'] = empresa['capital_social'].astype(float)
    else:
        print("A coluna 'capital_social' não existe no DataFrame.")
    # Tratamento do arquivo antes de inserir na base:
    empresa = empresa.reset_index()
    del empresa['index']

    for i in chunker(empresa.npartitions):
        df_chunk = empresa.get_partition(i)
        process_and_insert_chunk(df_chunk, conexao,'empresa')

    try:
        del empresa
    except:
        pass    

    print('Arquivos de empresa finalizados!')
    empresa_insert_end = time.time()
    empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
    print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))  

# %%
# Arquivos de estabelecimento:
estabelecimento_insert_start = time.time()
print("""
####################################
### Arquivos de ESTABELECIMENTO: ###
####################################
""")
i=0
logging.info(f"Ler arquivos de Estabelecimento")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS estabelecimento;')
conexao.commit()
for e in range(0, len(arquivos_estabelecimento)):
    print('Trabalhando no arquivo: '+arquivos_estabelecimento[e]+' [...]')
    column_names = ['cnpj_basico', 
                    'cnpj_ordem', 
                    'cnpj_dv', 
                    'identificador_matriz_filial', 
                    'nome_fantasia', 
                    'situacao_cadastral', 
                    'data_situacao_cadastral', 
                    'motivo_situacao_cadastral', 
                    'nome_cidade_exterior',
                    'pais',
                    'data_inicio_atividade',
                    'cnae_fiscal_principal',
                    'cnae_fiscal_secundaria',
                    'tipo_logradouro',
                    'logradouro',
                    'numero',
                    'complemento',
                    'bairro',
                    'cep',
                    'uf',
                    'municipio',
                    'ddd_1',
                    'telefone_1',
                    'ddd_2',
                    'telefone_2',
                    'ddd_fax',
                    'fax',
                    'correio_eletronico',
                    'situacao_especial',
                    'data_situacao_especial']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    estabelecimento = dd.DataFrame(column_data, 
                                    name='empresa_dataframe', 
                                    meta={'cnpj_basico': 'object', 
                                        'cnpj_ordem': 'object', 
                                        'cnpj_dv': 'object', 
                                        'identificador_matriz_filial': 'object', 
                                        'nome_fantasia': 'object', 
                                        'situacao_cadastral': 'object', 
                                        'data_situacao_cadastral': 'object', 
                                        'motivo_situacao_cadastral': 'object', 
                                        'nome_cidade_exterior': 'object',
                                        'pais': 'object',
                                        'data_inicio_atividade': 'object',
                                        'cnae_fiscal_principal': 'object',
                                        'cnae_fiscal_secundaria': 'object',
                                        'tipo_logradouro': 'object',
                                        'logradouro': 'object',
                                        'numero': 'object',
                                        'complemento': 'object',
                                        'bairro': 'object',
                                        'cep': 'object',
                                        'uf': 'object',
                                        'municipio': 'object',
                                        'ddd_1': 'object',
                                        'telefone_1': 'object',
                                        'ddd_2': 'object',
                                        'telefone_2': 'object',
                                        'ddd_fax': 'object',
                                        'fax': 'object',
                                        'correio_eletronico': 'object',
                                        'situacao_especial': 'object',
                                        'data_situacao_especial': 'object'}, 
                                        divisions=divisoes)
    # Reparticionar em 10 partições
    try:
        del estabelecimento
    except:
        pass
                  
    extracted_file_path = os.path.join(extracted_files, arquivos_estabelecimento[e])
    estabelecimento = dd.read_csv(extracted_file_path,
                                  sep=';',
                                  # nrows=100,
                                  skiprows=0,
                                  header=None,
                                  dtype='object',
                                  encoding='latin1')
    # Renomear Colunas
    estabelecimento.columns = column_names

    # Tratamento do arquivo antes de inserir na base:
    estabelecimento = estabelecimento.reset_index()
    del estabelecimento['index']

    # Gravar dados no banco:
    # estabelecimento
    for i in range(estabelecimento.npartitions):
        df_chunk = estabelecimento.get_partition(i)
        process_and_insert_chunk(df_chunk, conexao,'estabelecimento')

    try:
        del estabelecimento
    except:
        pass
    
    print('Arquivos de estabelecimento finalizados!')
    estabelecimento_insert_end = time.time()
    estabelecimento_Tempo_insert = round((estabelecimento_insert_end - estabelecimento_insert_start))
    print('Tempo de execução do processo de estabelecimento (em segundos): ' + str(estabelecimento_Tempo_insert))

# %%
# Arquivos de socios:
socios_insert_start = time.time()
print("""
#########################
## Arquivos de SOCIOS: ##
#########################
""")
i=0
logging.info(f"Ler arquivos de Socios")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS socios;')
conexao.commit()
for e in range(0, len(arquivos_socios)):
    print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
    column_names = ['cnpj_basico',
                      'identificador_socio',
                      'nome_socio_razao_social',
                      'cpf_cnpj_socio',
                      'qualificacao_socio',
                      'data_entrada_sociedade',
                      'pais',
                      'representante_legal',
                      'nome_do_representante',
                      'qualificacao_representante_legal',
                      'faixa_etaria']                      
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    socios = dd.DataFrame(column_data, 
                            name='empresa_dataframe', 
                            meta={'cnpj_basico': 'object',
                                'identificador_socio': 'object',
                                'nome_socio_razao_social': 'object',
                                'cpf_cnpj_socio': 'object',
                                'qualificacao_socio': 'object',
                                'data_entrada_sociedade': 'object',
                                'pais': 'object',
                                'representante_legal': 'object',
                                'nome_do_representante': 'object',
                                'qualificacao_representante_legal': 'object',
                                'faixa_etaria': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições
    try:
        del socios
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
    socios = dd.read_csv(extracted_file_path,
                         sep=';',
                         # nrows=100,
                         skiprows=0,
                         header=None,
                         dtype='object',
                         encoding='latin1')
    # Tratamento do arquivo antes de inserir na base:
    socios = socios.reset_index()
    del socios['index']

    # Renomear colunas
    socios.columns = column_names

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

# %%
# Arquivos de simples:
simples_insert_start = time.time()
print("""
###################################
## Arquivos do SIMPLES NACIONAL: ##
###################################
""")
i=0
logging.info(f"Ler arquivos de Simples Nacional")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS simples;')
conexao.commit()
for e in range(0, len(arquivos_simples)):
    print('Trabalhando no arquivo: '+arquivos_simples[e]+' [...]')
    column_names = ['cnpj_basico',
                    'opcao_pelo_simples',
                    'data_opcao_simples',
                    'data_exclusao_simples',
                    'opcao_mei',
                    'data_opcao_mei',
                    'data_exclusao_mei']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    simples = dd.DataFrame(column_data, 
                            name='simples_dataframe', 
                            meta={'cnpj_basico': 'object',
                                    'opcao_pelo_simples': 'object',
                                    'data_opcao_simples': 'object',
                                    'data_exclusao_simples': 'object',
                                    'opcao_mei': 'object',
                                    'data_opcao_mei': 'object',
                                    'data_exclusao_mei': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições
    try:
        del simples
    except:
        pass

    # Verificar tamanho do arquivo:
    print('Lendo o arquivo ' + arquivos_simples[e]+' [...]')
    extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])
    simples = dd.read_csv(extracted_file_path,
                        sep=';',
                        nrows=nrows,
                        skiprows=skiprows,
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

# %%
# Arquivos de cnae:
cnae_insert_start = time.time()
print("""
#######################
## Arquivos de cnae: ##
#######################
""")
i=0
logging.info(f"Ler arquivos de CNAE")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS cnae;')
conexao.commit()
for e in range(0, len(arquivos_cnae)):
    print('Trabalhando no arquivo: '+arquivos_cnae[e]+' [...]')
    column_names = ['codigo', 'descricao']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    cnae = dd.DataFrame(column_data, name='cnae_dataframe', meta={'codigo': 'object', 'descricao': 'object'}, divisions=divisoes)
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


# %%
# Arquivos de moti:
moti_insert_start = time.time()
print("""
############################################
## Arquivos de motivos da situação atual: ##
############################################
""")
i=0
logging.info(f"Ler arquivos de Situacao Atual")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS moti;')
conexao.commit()
for e in range(0, len(arquivos_moti)):
    print('Trabalhando no arquivo: '+arquivos_moti[e]+' [...]')
    column_names = ['codigo', 'descricao']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    moti = dd.DataFrame(column_data, name='empresa_dataframe', meta={'codigo': 'object', 'descricao': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições
    try:
        del moti
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_moti[e])
    moti = dd.read_csv(extracted_file_path,
                        sep=';',
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

# %%
# Arquivos de munic:
munic_insert_start = time.time()
print("""
##########################
## Arquivos de municípios:
##########################
""")
i=0
logging.info(f"Ler arquivos de Municipios")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS munic;')
conexao.commit()
for e in range(0, len(arquivos_munic)):
    print('Trabalhando no arquivo: '+arquivos_munic[e]+' [...]')
    column_names = ['codigo', 'descricao']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    munic = dd.DataFrame(column_data, name='empresa_dataframe', meta={'codigo': 'object', 'descricao': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições    
    try:
        del munic
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_munic[e])
    munic = dd.read_csv(extracted_file_path, 
                        sep=';',
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

# %%
# Arquivos de natju:
natju_insert_start = time.time()
print("""
####################################
## Arquivos de natureza jurídica: ##
####################################
""")
logging.info(f"Ler arquivos de Natureza Juridica")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS natju;')
conexao.commit()
for e in range(0, len(arquivos_natju)):
    print('Trabalhando no arquivo: '+arquivos_natju[e]+' [...]')
    column_names = ['codigo', 'descricao']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    natju = dd.DataFrame(column_data, name='natju_dataframe', meta={'codigo': 'object', 'descricao': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições    
    try:
        del natju
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_natju[e])
    natju = dd.read_csv(extracted_file_path, 
                        sep=';',
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
        process_and_insert_chunk(df_chunk, conexao,'emprenatjusa')     


    print('Arquivos de natju finalizados!')
    natju_insert_end = time.time()
    natju_Tempo_insert = round((natju_insert_end - natju_insert_start))
    print('Tempo de execução do processo de natureza jurídica (em segundos): ' +
      str(natju_Tempo_insert))

# %%
# Arquivos de pais:
pais_insert_start = time.time()
print("""
#######################
## Arquivos de país: ##
#######################
""")
i=0
logging.info(f"Ler arquivos de PAIS")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS pais;')
conexao.commit()
for e in range(0, len(arquivos_pais)):
    print('Trabalhando no arquivo: '+arquivos_pais[e]+' [...]')
    column_names = ['codigo', 'descricao']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    pais = dd.DataFrame(column_data, name='pais_dataframe', meta={'codigo': 'object', 'descricao': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições
    try:
        del pais
    except:
        pass

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

# %%
# Arquivos de qualificação de sócios:
quals_insert_start = time.time()
print("""
#########################################
## Arquivos de qualificação de sócios: ##
#########################################
""")
i=0
logging.info(f"Ler arquivos de Qualificacao de Socios")
# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS quals;')
conexao.commit()

for e in range(0, len(arquivos_quals)):
    print('Trabalhando no arquivo: '+arquivos_quals[e]+' [...]')
    column_names = ['codigo', 'descricao']
    column_data = {name: [] for name in column_names}
    num_particoes = 10
    divisoes = np.linspace(0, 10000, num=num_particoes + 1).tolist()
    quals = dd.DataFrame(column_data, name='quals_dataframe', meta={'codigo': 'object', 'descricao': 'object'}, divisions=divisoes)
    # Reparticionar em 10 partições
    try:
        del quals
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_quals[e])
    quals = dd.requalad_csv(extracted_file_path, 
                            sep=';',
                            skiprows=0, 
                            header=None, 
                            dtype='object', 
                            encoding='latin1')
    # Renomear colunas
    quals.columns = column_names
    # Tratamento do arquivo antes de inserir na base:
    quals = quals.reset_index()
    del quals['index']

    # Gravar dados no banco:
    # quals
    for i in range(quals.npartitions):
        df_chunk = quals.get_partition(i)
        process_and_insert_chunk(df_chunk, conexao,'quals')

    try:
        del quals
    except:
        pass
    print('Arquivos de quals finalizados!')
    quals_insert_end = time.time()
    quals_Tempo_insert = round((quals_insert_end - quals_insert_start))
    print('Tempo de execução do processo de qualificação de sócios (em segundos): ' + str(quals_Tempo_insert))


# %%
insert_end = time.time()
Tempo_insert = round((insert_end - insert_start))

print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")
logging.info(f"Processo de carga dos arquivos finalizado")
# Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)
print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert))


# %%
# Criar índices na base de dados:
index_start = time.time()
print("""
#######################################
## Criar índices na base de dados [...]
#######################################
""")
if cnpj_basico!="":
    cur.execute('CREATE INDEX empresa_cnpj ON empresa(cnpj_basico);')
    cur.execute('conexao.commit;')
    cur.execute('CREATE INDEX estabelecimento_cnpj ON estabelecimento(cnpj_basico);')
    cur.execute('conexao.commit;')
    cur.execute('CREATE INDEX socios_cnpj ON socios(cnpj_basico);')
    cur.execute('conexao.commit;')
    cur.execute('CREATE INDEX simples_cnpj ON simples(cnpj_basico);')
    cur.execute('conexao.commit;')
    conexao.commit()
    print("""
    ############################################################
    ## Índices criados nas tabelas, para a coluna `cnpj_basico`:
    - empresa
    - estabelecimento
    - socios
    - simples
    ############################################################
    """)
    index_end = time.time()
    index_time = round(index_end - index_start)
    print('Tempo para criar os índices (em segundos): ' + str(index_time))
    # Encerramento da thread (exemplo simplificado)
    thread.join()
    # %%
    print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
     - Desenvolvido por: Aphonso Henrique do Amaral Rafael
     - Adaptado por: Vander Ribeiro Elme
    - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
    """)
