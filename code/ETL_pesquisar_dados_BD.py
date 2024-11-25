from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine
import bs4 as bs
import ftplib
import os
import pandas as pd
import psycopg2
import re
import sys
import time
import requests
import urllib.request
import wget



#%%
# Ler arquivo de configuração de ambiente # https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
def getEnv(env):
    return os.getenv(env)

# print('Especifique o local do seu arquivo de configuração ".env". Por exemplo: C:\...\Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ\code')
# C:\Aphonso_C\Git\Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ\code
local_env = 'D:\\Repositorio\\00_Programação\\06 - DADOS_RFB\\DADOS_RFB\\code'
dotenv_path = os.path.join(local_env, '.env')
load_dotenv(dotenv_path=dotenv_path)

#%%
# Conectar no banco de dados:
# Dados da conexão com o BD
user=getEnv('DB_USER')
passw=getEnv('DB_PASSWORD')
host=getEnv('DB_HOST')
port=getEnv('DB_PORT')
database=getEnv('DB_NAME')
try:
    # Conectar:
    conn = psycopg2.connect('dbname='+database+' '+'user='+user+' '+'host='+host+' '+'port='+port+' '+'password='+passw)
    cur = conn.cursor()
    num = 0
    cur.execute("select ES.cnpj_basico FROM estabelecimento ES INNER JOIN empresa EM on ES.cnpj_basico=EM.cnpj_basico INNER JOIN munic MU on MU.codigo=ES.municipio WHERE (ES.nome_fantasia LIKE '%Condomínio%' or ES.nome_fantasia LIKE '%Edificio%' or ES.nome_fantasia LIKE '%Residencial%') AND MU.descricao = 'OSASCO'")
    #, ES.cnpj_ordem, ES.cnpj_dv, EM.razao_social, ES.nome_fantasia, EM.porte_empresa, ES.situacao_cadastral, ES.tipo_logradouro, ES.logradouro, ES.numero, ES.complemento, ES.bairro, ES.cep, ES.uf, MU.descricao, ES.ddd_1, ES.telefone_1, ES.ddd_2, ES.telefone_2, ES.correio_eletronico, ES.situacao_especial AND ES.cep LIKE '%06122%'
    resultado = cur.fetchall()
    
    for res in resultado:
        num = num + 1
        print(res)
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    print(num)
    conn.close()
    
# empresa.columns = ['cnpj_basico', 
#                    'razao_social', 
#                    'natureza_juridica', 
#                    'qualificacao_responsavel', 
#                    'capital_social', 
#                    'porte_empresa', 
#                    'ente_federativo_responsavel']
#estabelecimento.columns = ['cnpj_basico',
#                           'cnpj_ordem',
#                           'cnpj_dv',
#                           'identificador_matriz_filial',
#                           'nome_fantasia',
#                           'situacao_cadastral',
#                           'data_situacao_cadastral',
#                           'motivo_situacao_cadastral',
#                           'nome_cidade_exterior',
#                           'pais',
#                           'data_inicio_atividade',
#                           'cnae_fiscal_principal',
#                           'cnae_fiscal_secundaria',
#                           'tipo_logradouro',
#                           'logradouro',
#                           'numero',
#                           'complemento',
#                           'bairro',
#                           'cep',
#                           'uf',
#                           'municipio',
#                           'ddd_1',
#                           'telefone_1',
#                           'ddd_2',
#                           'telefone_2',
#                           'ddd_fax',
#                           'fax',
#                           'correio_eletronico',
#                           'situacao_especial',
#                           'data_situacao_especial']
#socios.columns = ['cnpj_basico',
#                  'identificador_socio',
#                  'nome_socio_razao_social',
#                  'cpf_cnpj_socio',
#                  'qualificacao_socio',
#                  'data_entrada_sociedade',
#                  'pais',
#                  'representante_legal',
#                  'nome_do_representante',
#                  'qualificacao_representante_legal',
#                  'faixa_etaria']
#simples.columns = ['cnpj_basico',
#                   'opcao_pelo_simples',
#                   'data_opcao_simples',
#                   'data_exclusao_simples',
#                   'opcao_mei',
#                   'data_opcao_mei',
#                   'data_exclusao_mei']
#cnae.columns = ['codigo',
#                'descricao']

#moti.columns = ['codigo',
#                'descricao']
#munic.columns = ['codigo',
#                 'descricao']

#natju.columns = ['codigo',
#                 'descricao']

#pais.columns = ['codigo', 
#                'descricao']
# 
# Arquivos de qualificação de sócios:
#quals.columns = ['codigo', 
#                 'descricao']

#Relatorio
#estabelecimento.cnpj_basico, estabelecimento.cnpj_ordem, estabelecimento.cnpj_dv, empresa.razao_social, estabelecimento.nome_fantasia, 
#empresa.porte_empresa, estabelecimento.situacao_cadastral, estabelecimento.tipo_logradouro, estabelecimento.logradouro,
#estabelecimento.numero, estabelecimento.complemento, estabelecimento.bairro, estabelecimento.cep, estabelecimento.uf, estabelecimento.municipio,
#estabelecimento.ddd_1, estabelecimento.telefone_1, estabelecimento.ddd_2, estabelecimento.telefone_2, estabelecimento.correio_eletronico,
#estabelecimento.situacao_especial',



