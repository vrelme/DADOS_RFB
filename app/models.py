# app/models.py

from datetime import datetime

from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    Float,
    DateTime,
    Text,
    Index,
    PrimaryKeyConstraint
)

from app.database import Base


# =====================================================
# EMPRESA
# =====================================================
class Empresa(Base):
    __tablename__ = "empresa"

    cnpj_basico = Column(String(8), primary_key=True)

    razao_social = Column(String(255), nullable=False, index=True)

    natureza_juridica = Column(Integer)

    qualificacao_responsavel = Column(Integer)

    capital_social = Column(Float)

    porte_empresa = Column(Integer)

    ente_federativo = Column(String(255))

    __table_args__ = (
        Index("idx_empresa_razao_social", "razao_social"),
    )


# =====================================================
# EMPRESA STAGING
# =====================================================
class EmpresaStaging(Base):
    __tablename__ = "empresa_staging"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    cnpj_basico = Column(String(8))

    razao_social = Column(String(255))

    natureza_juridica = Column(Integer)

    qualificacao_responsavel = Column(Integer)

    capital_social = Column(Float)

    porte_empresa = Column(Integer)

    ente_federativo = Column(String(255))


# =====================================================
# ESTABELECIMENTO
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

    cnae_fiscal_secundaria = Column(Text)

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

        Index("idx_estab_nome_fantasia", "nome_fantasia"),

        Index("idx_estab_cnae", "cnae_fiscal_principal"),

        Index("idx_estab_uf", "uf"),

        Index("idx_estab_municipio", "municipio"),
    )


# =====================================================
# ESTABELECIMENTO STAGING
# =====================================================
class EstabelecimentoStaging(Base):
    __tablename__ = "estabelecimento_staging"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    cnpj_basico = Column(String(8))

    cnpj_ordem = Column(String(4))

    cnpj_dv = Column(String(2))

    identificador_matriz_filial = Column(String(1))

    nome_fantasia = Column(String(255))

    situacao_cadastral = Column(String(2))

    data_situacao_cadastral = Column(String(8))

    motivo_situacao_cadastral = Column(String(2))

    nome_cidade_exterior = Column(String(255))

    pais = Column(String(3))

    data_inicio_atividade = Column(String(8))

    cnae_fiscal_principal = Column(String(7))

    cnae_fiscal_secundaria = Column(Text)

    tipo_logradouro = Column(String(50))

    logradouro = Column(String(255))

    numero = Column(String(20))

    complemento = Column(String(255))

    bairro = Column(String(100))

    cep = Column(String(8))

    uf = Column(String(2))

    municipio = Column(String(4))

    ddd1 = Column(String(4))

    telefone1 = Column(String(20))

    ddd2 = Column(String(4))

    telefone2 = Column(String(20))

    ddd_fax = Column(String(4))

    fax = Column(String(20))

    email = Column(String(255))

    situacao_especial = Column(String(255))

    data_situacao_especial = Column(String(8))


# =====================================================
# SOCIO
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

    __table_args__ = (
        Index("idx_socio_cnpj_basico", "cnpj_basico"),

        Index("idx_socio_nome", "nome_socio"),

        Index("idx_socio_documento", "cpf_cnpj_socio"),
    )


# =====================================================
# SOCIO STAGING
# =====================================================
class SocioStaging(Base):
    __tablename__ = "socio_staging"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    cnpj_basico = Column(String(8))

    identificador_socio = Column(String(1))

    nome_socio = Column(String(255))

    cpf_cnpj_socio = Column(String(14))

    qualificacao_socio = Column(String(2))

    data_entrada_sociedade = Column(String(8))

    pais = Column(String(3))

    representante_legal = Column(String(11))

    nome_representante = Column(String(255))

    qualificacao_representante_legal = Column(String(2))

    faixa_etaria = Column(String(1))


# =====================================================
# SIMPLES
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
# ETL EXECUTION
# =====================================================
class ETLExecution(Base):
    __tablename__ = "etl_execution"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    pipeline = Column(String(100), nullable=False)

    file_name = Column(String(255), nullable=False, index=True)

    table_name = Column(String(100), nullable=False)

    status = Column(String(20), nullable=False, index=True)

    records_processed = Column(BigInteger, default=0)

    started_at = Column(DateTime, default=datetime.utcnow)

    finished_at = Column(DateTime)

    duration_seconds = Column(Integer)

    worker = Column(String(100))

    environment = Column(String(50))

    error_message = Column(Text)

    retry_count = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_etl_status", "status"),

        Index("idx_etl_pipeline", "pipeline"),

        Index("idx_etl_started", "started_at"),
    )


# =====================================================
# ETL RUN / OPERATIONAL STATUS
# =====================================================
class ETLRun(Base):
    __tablename__ = "etl_run"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    pipeline = Column(String(100), nullable=False, index=True)

    status = Column(String(30), nullable=False, index=True)

    sync_strategy = Column(String(50))

    load_target = Column(String(50))

    active_database = Column(String(100))

    current_phase = Column(String(100), index=True)

    current_table = Column(String(100), index=True)

    current_file = Column(String(255))

    total_files = Column(Integer, default=0)

    completed_files = Column(Integer, default=0)

    failed_files = Column(Integer, default=0)

    total_records = Column(BigInteger, default=0)

    progress_percent = Column(Float, default=0)

    started_at = Column(DateTime, default=datetime.utcnow, index=True)

    heartbeat_at = Column(DateTime, default=datetime.utcnow, index=True)

    estimated_finish_at = Column(DateTime)

    finished_at = Column(DateTime)

    duration_seconds = Column(Integer)

    error_message = Column(Text)

    __table_args__ = (
        Index("idx_etl_run_status_started", "status", "started_at"),
    )


class ETLRunPhase(Base):
    __tablename__ = "etl_run_phase"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    run_id = Column(BigInteger, nullable=False, index=True)

    phase_name = Column(String(100), nullable=False, index=True)

    table_name = Column(String(100), index=True)

    status = Column(String(30), nullable=False, index=True)

    started_at = Column(DateTime, default=datetime.utcnow, index=True)

    finished_at = Column(DateTime)

    duration_seconds = Column(Integer)

    message = Column(Text)


class ETLFileProgress(Base):
    __tablename__ = "etl_file_progress"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    run_id = Column(BigInteger, nullable=False, index=True)

    pipeline = Column(String(100), nullable=False, index=True)

    table_name = Column(String(100), nullable=False, index=True)

    file_name = Column(String(255), nullable=False, index=True)

    status = Column(String(30), nullable=False, index=True)

    records_processed = Column(BigInteger, default=0)

    started_at = Column(DateTime, default=datetime.utcnow, index=True)

    finished_at = Column(DateTime)

    duration_seconds = Column(Integer)

    throughput_rows_per_second = Column(Float, default=0)

    error_message = Column(Text)

    __table_args__ = (
        Index("idx_file_progress_run_status", "run_id", "status"),
    )


# =====================================================
# ETL METRICS - GRAFANA
# =====================================================
class ETLMetric(Base):
    __tablename__ = "etl_metric"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    pipeline = Column(String(100), nullable=False, index=True)

    table_name = Column(String(100), index=True)

    file_name = Column(String(255), index=True)

    metric_name = Column(String(100), nullable=False, index=True)

    metric_value = Column(Float, nullable=False)

    unit = Column(String(50))

    worker = Column(String(100))

    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("idx_metric_name_created", "metric_name", "created_at"),
    )


# =====================================================
# ETL CHECKPOINT / RESUME
# =====================================================
class ETLCheckpoint(Base):
    __tablename__ = "etl_checkpoint"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    pipeline = Column(String(100), nullable=False, index=True)

    table_name = Column(String(100), nullable=False, index=True)

    file_name = Column(String(255), nullable=False, index=True)

    status = Column(String(20), nullable=False, index=True)

    records_processed = Column(BigInteger, default=0)

    checksum = Column(String(128))

    error_message = Column(Text)

    updated_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("idx_checkpoint_file_status", "file_name", "status"),
    )


# =====================================================
# DEAD LETTER QUEUE
# =====================================================
class ETLDeadLetter(Base):
    __tablename__ = "etl_dead_letter"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    pipeline = Column(String(100), nullable=False, index=True)

    table_name = Column(String(100), nullable=False, index=True)

    file_name = Column(String(255), nullable=False, index=True)

    row_number = Column(BigInteger)

    reason_code = Column(String(100), nullable=False, index=True)

    reason_message = Column(Text)

    raw_payload = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)


# =====================================================
# DATA QUALITY RULES
# =====================================================
class DataQualityRule(Base):
    __tablename__ = "data_quality_rule"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    table_name = Column(String(100), nullable=False, index=True)

    column_name = Column(String(100), nullable=False)

    rule_type = Column(String(50), nullable=False)

    rule_expression = Column(String(255), nullable=False)

    severity = Column(String(20), default="ERROR")

    enabled = Column(Integer, default=1, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    updated_at = Column(DateTime, default=datetime.utcnow)


# =====================================================
# TABELAS AUXILIARES
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