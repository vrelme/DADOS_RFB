# app/models.py

from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    PrimaryKeyConstraint,
    Index
)

from app.database import Base


# =====================================================
# EMPRESA
# Raiz do CNPJ (8 dígitos)
# =====================================================
class Empresa(Base):
    __tablename__ = "empresa"

    cnpj_basico = Column(String(8), primary_key=True)

    razao_social = Column(String(255), nullable=False, index=True)
    natureza_juridica = Column(String(4))
    qualificacao_responsavel = Column(String(2))
    capital_social = Column(String(20))
    porte_empresa = Column(String(2))
    ente_federativo = Column(String(255))


# =====================================================
# ESTABELECIMENTO
# CNPJ completo = básico + ordem + dv
# Uma empresa pode ter vários estabelecimentos
# =====================================================
class Estabelecimento(Base):
    __tablename__ = "estabelecimento"

    cnpj_basico = Column(String(8), nullable=False)
    cnpj_ordem = Column(String(4), nullable=False)
    cnpj_dv = Column(String(2), nullable=False)

    identificador_matriz_filial = Column(String(1))
    nome_fantasia = Column(String(255), index=True)

    situacao_cadastral = Column(String(2))
    data_situacao_cadastral = Column(String(8))
    motivo_situacao_cadastral = Column(String(2))

    nome_cidade_exterior = Column(String(255))
    pais = Column(String(3))

    data_inicio_atividade = Column(String(8))

    cnae_fiscal_principal = Column(String(7), index=True)
    cnae_fiscal_secundaria = Column(String(1000))

    tipo_logradouro = Column(String(50))
    logradouro = Column(String(255))
    numero = Column(String(20))
    complemento = Column(String(255))
    bairro = Column(String(100))
    cep = Column(String(8))

    uf = Column(String(2), index=True)
    municipio = Column(String(4), index=True)

    ddd1 = Column(String(4))
    telefone1 = Column(String(20))

    ddd2 = Column(String(4))
    telefone2 = Column(String(20))

    ddd_fax = Column(String(4))
    fax = Column(String(20))

    email = Column(String(255), index=True)

    situacao_especial = Column(String(255))
    data_situacao_especial = Column(String(8))

    __table_args__ = (
        PrimaryKeyConstraint(
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv"
        ),
        Index("idx_estab_cnpj_basico", "cnpj_basico"),
    )


# =====================================================
# SOCIO
# Muitos sócios por empresa
# =====================================================
class Socio(Base):
    __tablename__ = "socio"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    cnpj_basico = Column(String(8), nullable=False, index=True)

    identificador_socio = Column(String(1))
    nome_socio = Column(String(255), index=True)

    cpf_cnpj_socio = Column(String(14), index=True)

    qualificacao_socio = Column(String(2))
    data_entrada_sociedade = Column(String(8))

    pais = Column(String(3))

    representante_legal = Column(String(11))
    nome_representante = Column(String(255))

    qualificacao_representante_legal = Column(String(2))
    faixa_etaria = Column(String(1))


# =====================================================
# SIMPLES
# Regime tributário
# =====================================================
class Simples(Base):
    __tablename__ = "simples"

    cnpj_basico = Column(String(8), primary_key=True)

    opcao_simples = Column(String(1))
    data_opcao_simples = Column(String(8))
    data_exclusao_simples = Column(String(8))

    opcao_mei = Column(String(1))
    data_opcao_mei = Column(String(8))
    data_exclusao_mei = Column(String(8))


# =====================================================
# TABELAS AUXILIARES / DOMÍNIO
# =====================================================
class CNAE(Base):
    __tablename__ = "cnae"

    codigo = Column(String(7), primary_key=True)
    descricao = Column(String(255), nullable=False, index=True)


class Municipio(Base):
    __tablename__ = "municipio"

    codigo = Column(String(4), primary_key=True)
    descricao = Column(String(255), nullable=False, index=True)


class NaturezaJuridica(Base):
    __tablename__ = "natureza_juridica"

    codigo = Column(String(4), primary_key=True)
    descricao = Column(String(255), nullable=False)


class Pais(Base):
    __tablename__ = "pais"

    codigo = Column(String(3), primary_key=True)
    descricao = Column(String(255), nullable=False)


class QualificacaoSocio(Base):
    __tablename__ = "qualificacao_socio"

    codigo = Column(String(2), primary_key=True)
    descricao = Column(String(255), nullable=False)


class MotivoSituacao(Base):
    __tablename__ = "motivo_situacao"

    codigo = Column(String(2), primary_key=True)
    descricao = Column(String(255), nullable=False)