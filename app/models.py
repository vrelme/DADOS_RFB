from sqlalchemy import Column, String, Integer, Numeric, Date
from app.database import Base

class Empresa(Base):
    __tablename__ = "empresa"

    cnpj_basico = Column(String(14), primary_key=True)
    razao_social = Column(String(255))
    natureza_juridica = Column(Integer)
    qualificacao_responsavel = Column(Integer)
    capital_social = Column(Numeric(15,2))
    porte_empresa = Column(Integer)


class Estabelecimento(Base):
    __tablename__ = "estabelecimento"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cnpj_basico = Column(String(14), index=True)
    nome_fantasia = Column(String(255))
    situacao_cadastral = Column(Integer)
    data_inicio_atividade = Column(Date)
    uf = Column(String(2))
    municipio = Column(Integer)


class Socio(Base):
    __tablename__ = "socios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cnpj_basico = Column(String(14), index=True)
    nome_socio_razao_social = Column(String(255))
    qualificacao_socio = Column(Integer)