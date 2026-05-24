# app/etl/rfb_manifest.py

from dataclasses import dataclass


@dataclass(frozen=True)
class RFBTableDefinition:
    table_name: str
    patterns: tuple[str, ...]
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]


RFB_TABLES = (
    RFBTableDefinition(
        table_name="empresa",
        patterns=("*.EMPRECSV",),
        columns=(
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo",
        ),
        key_columns=("cnpj_basico",),
    ),
    RFBTableDefinition(
        table_name="estabelecimento",
        patterns=("*.ESTABELE",),
        columns=(
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "identificador_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "ddd1",
            "telefone1",
            "ddd2",
            "telefone2",
            "ddd_fax",
            "fax",
            "email",
            "situacao_especial",
            "data_situacao_especial",
        ),
        key_columns=("cnpj_basico", "cnpj_ordem", "cnpj_dv"),
    ),
    RFBTableDefinition(
        table_name="socio",
        patterns=("*.SOCIOCSV",),
        columns=(
            "cnpj_basico",
            "identificador_socio",
            "nome_socio",
            "cpf_cnpj_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "representante_legal",
            "nome_representante",
            "qualificacao_representante_legal",
            "faixa_etaria",
        ),
        key_columns=(
            "cnpj_basico",
            "identificador_socio",
            "nome_socio",
            "cpf_cnpj_socio",
            "data_entrada_sociedade",
        ),
    ),
    RFBTableDefinition(
        table_name="simples",
        patterns=("*.SIMPLES.CSV.*", "*.SIMPLES.CSV*"),
        columns=(
            "cnpj_basico",
            "opcao_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ),
        key_columns=("cnpj_basico",),
    ),
    RFBTableDefinition(
        table_name="cnae",
        patterns=("*.CNAECSV",),
        columns=("codigo", "descricao"),
        key_columns=("codigo",),
    ),
    RFBTableDefinition(
        table_name="moti",
        patterns=("*.MOTICSV",),
        columns=("codigo", "descricao"),
        key_columns=("codigo",),
    ),
    RFBTableDefinition(
        table_name="munic",
        patterns=("*.MUNICCSV",),
        columns=("codigo", "descricao"),
        key_columns=("codigo",),
    ),
    RFBTableDefinition(
        table_name="natju",
        patterns=("*.NATJUCSV",),
        columns=("codigo", "descricao"),
        key_columns=("codigo",),
    ),
    RFBTableDefinition(
        table_name="pais",
        patterns=("*.PAISCSV",),
        columns=("codigo", "descricao"),
        key_columns=("codigo",),
    ),
    RFBTableDefinition(
        table_name="quals",
        patterns=("*.QUALSCSV",),
        columns=("codigo", "descricao"),
        key_columns=("codigo",),
    ),
)


RFB_TABLES_BY_NAME = {table.table_name: table for table in RFB_TABLES}


def get_rfb_table(table_name: str) -> RFBTableDefinition:
    return RFB_TABLES_BY_NAME[table_name]


def raw_import_create_table_sql(table: RFBTableDefinition) -> str:
    columns_sql = ",\n            ".join(
        f"{column} TEXT NULL" if column.endswith("descricao") or column in {"cnae_fiscal_secundaria"}
        else f"{column} VARCHAR(255) NULL"
        for column in table.columns
    )
    return f"""
        CREATE TABLE IF NOT EXISTS {table.table_name} (
            {columns_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
