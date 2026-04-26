import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from requests import session
import requests

from src.database import SessionLocal


class LoaderService:

    def __init__(self, extract_dir):

        
        self.extract_dir = Path(extract_dir)
        self.max_workers = 4

    # ==================================================
    # MÉTODO PRINCIPAL
    # ==================================================

    def run(self):

        arquivos = list(self.extract_dir.iterdir())

        tarefas = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            for arquivo in arquivos:
                future = executor.submit(self.process_file_safe, arquivo)
                tarefas.append(future)

            for future in as_completed(tarefas):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Falha worker: {e}")

        ordem = [
            "pais",
            "munic",
            "quals",
            "natju",
            "cnae",
            "empresa",
            "estabelecimento",
            "socios",
            "simples"
        ]

        arquivos = list(self.extract_dir.iterdir())

        for tabela in ordem:

            logging.info(f"Iniciando carga: {tabela.upper()}")

            arquivos_tabela = self.filter_files(arquivos, tabela)

            for arquivo in arquivos_tabela:
                self.process_file(tabela, arquivo)

            logging.info(f"Finalizado: {tabela.upper()}")

    # ==================================================
    # FILTRA ARQUIVOS
    # ==================================================

    def filter_files(self, arquivos, tabela):

        mapa = {
            "pais": "PAIS",
            "munic": "MUNIC",
            "quals": "QUALS",
            "natju": "NATJU",
            "cnae": "CNAE",
            "empresa": "EMPRE",
            "estabelecimento": "ESTABELE",
            "socios": "SOCIO",
            "simples": "SIMPLES"
        }

        chave = mapa[tabela]

        return [a for a in arquivos if chave in a.name.upper()]

    # ==================================================
    # PROCESSA ARQUIVO
    # ==================================================

    def process_file(self, tabela, arquivo):

        logging.info(f"Lendo arquivo {arquivo.name}")

        columns = self.get_columns(tabela)

        for chunk in pd.read_csv(
            arquivo,
            sep=";",
            names=columns,
            header=None,
            dtype="string",
            chunksize=50000,
            encoding="latin1",
            keep_default_na=False
        ):

            rows = chunk.to_dict("records")

            self.repositories[tabela].bulk_insert(rows)
            self.repositories[tabela].commit()

            logging.info(f"{len(rows)} registros inseridos em {tabela}")
            
    # ==================================================
    # PWORKER SEGURO
    # ==================================================

    def process_file_safe(self, arquivo):

        requests.session = SessionLocal()

        try:
            self.process_file(session, arquivo)

        except Exception as e:
            session.rollback()
            logging.error(f"Erro {arquivo.name}: {e}")

        finally:
            session.close()


    # ==================================================
    # COLUNAS
    # ==================================================

    def get_columns(self, tabela):

        estrutura = {
            "pais": ["codigo", "nome"],
            "munic": ["codigo", "nome"],
            "quals": ["codigo", "nome"],
            "natju": ["codigo", "nome"],
            "cnae": ["codigo", "nome"],

            "empresa": [
                "cnpj_basico",
                "razao_social",
                "natureza_juridica",
                "qualificacao_responsavel",
                "capital_social",
                "porte_empresa",
                "ente_federativo_responsavel"
            ],

            "estabelecimento": [
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "identificador_matriz_filial",
                "nome_fantasia",
                "situacao_cadastral"
            ],

            "socios": [
                "cnpj_basico",
                "identificador_socio",
                "nome_socio_razao_social"
            ],

            "simples": [
                "cnpj_basico",
                "opcao_simples"
            ]
        }

        return estrutura[tabela]
    
    # ==================================================
    # SCHEMA
    # ==================================================
    
    def get_schema(self, tabela):

        schemas = {

            "pais": {
                "codigo": "int",
                "nome": "text"
            },

            "empresa": {
                "cnpj_basico": "text",
                "razao_social": "text",
                "natureza_juridica": "int",
                "capital_social": "decimal"
            },

            "estabelecimento": {
                "cnpj_basico": "text",
                "situacao_cadastral": "int",
                "data_inicio_atividade": "date"
            }

        }

        return schemas[tabela]
