# app/etl/merge_repository.py

import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class MergeRepository:

    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # EMPRESA
    # =====================================================
    def merge_empresa(self):

        logger.info("MERGE EMPRESA iniciado")

        sql = text("""

        INSERT INTO empresa (
            cnpj_basico,
            razao_social,
            natureza_juridica,
            qualificacao_responsavel,
            capital_social,
            porte_empresa,
            ente_federativo
        )

        SELECT
            s.cnpj_basico,
            s.razao_social,
            s.natureza_juridica,
            s.qualificacao_responsavel,
            s.capital_social,
            s.porte_empresa,
            s.ente_federativo

        FROM empresa_staging s

        WHERE s.cnpj_basico IS NOT NULL

        ON DUPLICATE KEY UPDATE

            razao_social = VALUES(razao_social),
            natureza_juridica = VALUES(natureza_juridica),
            qualificacao_responsavel = VALUES(qualificacao_responsavel),
            capital_social = VALUES(capital_social),
            porte_empresa = VALUES(porte_empresa),
            ente_federativo = VALUES(ente_federativo)

        """)

        self.db.execute(sql)
        self.db.commit()

        logger.info("MERGE EMPRESA finalizado")

    # =====================================================
    # ESTABELECIMENTO
    # =====================================================
    def merge_estabelecimento(self):

        logger.info("MERGE ESTABELECIMENTO iniciado")

        sql = text("""

        INSERT INTO estabelecimento (

            cnpj_basico,
            cnpj_ordem,
            cnpj_dv,

            identificador_matriz_filial,
            nome_fantasia,

            situacao_cadastral,
            data_situacao_cadastral,
            motivo_situacao_cadastral,

            nome_cidade_exterior,
            pais,

            data_inicio_atividade,

            cnae_fiscal_principal,
            cnae_fiscal_secundaria,

            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cep,

            uf,
            municipio,

            ddd1,
            telefone1,

            ddd2,
            telefone2,

            ddd_fax,
            fax,

            email,

            situacao_especial,
            data_situacao_especial
        )

        SELECT

            s.cnpj_basico,
            s.cnpj_ordem,
            s.cnpj_dv,

            s.identificador_matriz_filial,
            s.nome_fantasia,

            s.situacao_cadastral,
            s.data_situacao_cadastral,
            s.motivo_situacao_cadastral,

            s.nome_cidade_exterior,
            s.pais,

            s.data_inicio_atividade,

            s.cnae_fiscal_principal,
            s.cnae_fiscal_secundaria,

            s.tipo_logradouro,
            s.logradouro,
            s.numero,
            s.complemento,
            s.bairro,
            s.cep,

            s.uf,
            s.municipio,

            s.ddd1,
            s.telefone1,

            s.ddd2,
            s.telefone2,

            s.ddd_fax,
            s.fax,

            s.email,

            s.situacao_especial,
            s.data_situacao_especial

        FROM estabelecimento_staging s

        WHERE s.cnpj_basico IS NOT NULL

        ON DUPLICATE KEY UPDATE

            nome_fantasia = VALUES(nome_fantasia),

            situacao_cadastral = VALUES(situacao_cadastral),
            data_situacao_cadastral = VALUES(data_situacao_cadastral),

            cnae_fiscal_principal = VALUES(cnae_fiscal_principal),

            logradouro = VALUES(logradouro),
            numero = VALUES(numero),
            bairro = VALUES(bairro),
            cep = VALUES(cep),

            uf = VALUES(uf),
            municipio = VALUES(municipio),

            telefone1 = VALUES(telefone1),

            email = VALUES(email)

        """)

        self.db.execute(sql)
        self.db.commit()

        logger.info("MERGE ESTABELECIMENTO finalizado")

    # =====================================================
    # SOCIO
    # =====================================================
    def merge_socio(self):

        logger.info("MERGE SOCIO iniciado")

        sql = text("""

        INSERT INTO socio (

            cnpj_basico,
            identificador_socio,
            nome_socio,
            cpf_cnpj_socio,
            qualificacao_socio,
            data_entrada_sociedade,
            pais,
            representante_legal,
            nome_representante,
            qualificacao_representante_legal,
            faixa_etaria

        )

        SELECT

            s.cnpj_basico,
            s.identificador_socio,
            s.nome_socio,
            s.cpf_cnpj_socio,
            s.qualificacao_socio,
            s.data_entrada_sociedade,
            s.pais,
            s.representante_legal,
            s.nome_representante,
            s.qualificacao_representante_legal,
            s.faixa_etaria

        FROM socio_staging s

        WHERE s.cnpj_basico IS NOT NULL

        """)

        self.db.execute(sql)
        self.db.commit()

        logger.info("MERGE SOCIO finalizado")

    # =====================================================
    # LIMPEZA STAGING
    # =====================================================
    def truncate_staging(self):

        logger.info("Limpando tabelas staging")

        self.db.execute(text("TRUNCATE TABLE empresa_staging"))
        self.db.execute(text("TRUNCATE TABLE estabelecimento_staging"))
        self.db.execute(text("TRUNCATE TABLE socio_staging"))

        self.db.commit()

        logger.info("Staging limpa")

    # =====================================================
    # EXECUÇÃO COMPLETA
    # =====================================================
    def execute_all_merges(self):

        logger.info("=" * 107)
        logger.info("INICIANDO PROCESSO DE MERGE")
        logger.info("=" * 107)

        self.merge_empresa()
        self.merge_estabelecimento()
        self.merge_socio()

        self.truncate_staging()

        logger.info("=" * 107)
        logger.info("MERGE FINALIZADO")
        logger.info("=" * 107)