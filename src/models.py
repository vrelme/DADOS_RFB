"""
models.py
=========================================================
Modelos ORM completos do projeto RFB Loader Enterprise 2.0

Tecnologia:
- SQLAlchemy 2.x
- Declarative ORM

Objetivo:
Representar todas as tabelas principais da base pública
da Receita Federal (CNPJ).

Compatível com:
- MySQL
- MariaDB
- PostgreSQL
- SQLite
- SQL Server (ajustes mínimos)

Autor base: Vander Elme
Refatorado com apoio IA ChatGPT
=========================================================
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Date, Numeric, Text


# =====================================================
# BASE ORM
# =====================================================

class Base(DeclarativeBase):
    """Classe base do ORM."""
    pass


# =====================================================
# TABELA: PAIS
# =====================================================

class Pais(Base):
    __tablename__ = "pais"

    codigo: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(String(255), nullable=False)


# =====================================================
# TABELA: MUNIC
# =====================================================

class Munic(Base):
    __tablename__ = "munic"

    codigo: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(String(255), nullable=False)


# =====================================================
# TABELA: QUALS
# =====================================================

class Quals(Base):
    __tablename__ = "quals"

    codigo: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(String(255), nullable=False)


# =====================================================
# TABELA: NATJU
# =====================================================

class Natju(Base):
    __tablename__ = "natju"

    codigo: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(String(255), nullable=False)


# =====================================================
# TABELA: CNAE
# =====================================================

class Cnae(Base):
    __tablename__ = "cnae"

    codigo: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(String(255), nullable=False)


# =====================================================
# TABELA: EMPRESA
# =====================================================

class Empresa(Base):
    __tablename__ = "empresa"

    cnpj_basico: Mapped[str] = mapped_column(String(14), primary_key=True)

    razao_social: Mapped[str] = mapped_column(String(255))
    natureza_juridica: Mapped[int] = mapped_column(Integer)
    qualificacao_responsavel: Mapped[int] = mapped_column(Integer)

    capital_social: Mapped[float] = mapped_column(Numeric(15, 2))

    porte_empresa: Mapped[int] = mapped_column(Integer)

    ente_federativo_responsavel: Mapped[str] = mapped_column(String(255))


# =====================================================
# TABELA: ESTABELECIMENTO
# =====================================================

class Estabelecimento(Base):
    __tablename__ = "estabelecimento"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    cnpj_basico: Mapped[str] = mapped_column(String(14))
    cnpj_ordem: Mapped[str] = mapped_column(String(4))
    cnpj_dv: Mapped[str] = mapped_column(String(2))

    identificador_matriz_filial: Mapped[int] = mapped_column(Integer)

    nome_fantasia: Mapped[str] = mapped_column(String(255))

    situacao_cadastral: Mapped[int] = mapped_column(Integer)
    data_situacao_cadastral: Mapped[Date] = mapped_column(Date)

    motivo_situacao_cadastral: Mapped[str] = mapped_column(String(255))

    nome_cidade_exterior: Mapped[str] = mapped_column(String(255))
    pais: Mapped[int] = mapped_column(Integer)

    data_inicio_atividade: Mapped[Date] = mapped_column(Date)

    cnae_fiscal_principal: Mapped[int] = mapped_column(Integer)
    cnae_fiscal_secundaria: Mapped[str] = mapped_column(Text)

    tipo_logradouro: Mapped[str] = mapped_column(String(100))
    logradouro: Mapped[str] = mapped_column(String(255))
    numero: Mapped[str] = mapped_column(String(20))
    complemento: Mapped[str] = mapped_column(String(255))
    bairro: Mapped[str] = mapped_column(String(255))
    cep: Mapped[str] = mapped_column(String(8))

    uf: Mapped[str] = mapped_column(String(2))
    municipio: Mapped[int] = mapped_column(Integer)

    ddd_1: Mapped[str] = mapped_column(String(4))
    telefone_1: Mapped[str] = mapped_column(String(20))

    ddd_2: Mapped[str] = mapped_column(String(4))
    telefone_2: Mapped[str] = mapped_column(String(20))

    dd_fax: Mapped[str] = mapped_column(String(4))
    fax: Mapped[str] = mapped_column(String(20))

    correio_eletronico: Mapped[str] = mapped_column(String(255))

    situacao_especial: Mapped[str] = mapped_column(String(255))
    data_situacao_especial: Mapped[Date] = mapped_column(Date)


# =====================================================
# TABELA: SOCIOS
# =====================================================

class Socios(Base):
    __tablename__ = "socios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    cnpj_basico: Mapped[str] = mapped_column(String(14))

    identificador_socio: Mapped[int] = mapped_column(Integer)

    nome_socio_razao_social: Mapped[str] = mapped_column(String(255))

    cpf_cnpj_socio: Mapped[str] = mapped_column(String(14))

    qualificacao_socio: Mapped[int] = mapped_column(Integer)

    data_entrada_sociedade: Mapped[Date] = mapped_column(Date)

    pais: Mapped[int] = mapped_column(Integer)

    representante_legal: Mapped[str] = mapped_column(String(255))

    nome_do_representante: Mapped[str] = mapped_column(String(255))

    qualificacao_representante_legal: Mapped[int] = mapped_column(Integer)

    faixa_etaria: Mapped[int] = mapped_column(Integer)


# =====================================================
# TABELA: SIMPLES
# =====================================================

class Simples(Base):
    __tablename__ = "simples"

    cnpj_basico: Mapped[str] = mapped_column(String(14), primary_key=True)

    opcao_simples: Mapped[str] = mapped_column(String(1))
    data_opcao_simples: Mapped[Date] = mapped_column(Date)
    data_exclusao_simples: Mapped[Date] = mapped_column(Date)

    opcao_mei: Mapped[str] = mapped_column(String(3))
    data_opcao_mei: Mapped[Date] = mapped_column(Date)
    data_exclusao_mei: Mapped[Date] = mapped_column(Date)