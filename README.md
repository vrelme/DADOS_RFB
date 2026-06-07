# Dados Publicos CNPJ - RFB Loader Enterprise

Processo ETL para carga dos dados publicos do CNPJ disponibilizados pela Receita Federal do Brasil.

Fonte oficial e layout dos arquivos: [metadados da RFB](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

## Objetivo

A v3 separa a carga bruta dos arquivos, o controle operacional e a base final consultavel:

```text
rfb_import    -> carga bruta dos arquivos CSV da RFB
dados_rfb     -> base final promovida/consultavel
dados_rfb_ops -> controle operacional do ETL
```

Esse desenho evita misturar metadados do processo com dados de negocio e reduz bloqueios no banco final durante cargas grandes.

## Arquivos Processados

O ETL identifica os arquivos descompactados em ordem alfabetica por nome e carrega cada grupo na tabela correspondente:

| Padrao do arquivo | Tabela destino |
| --- | --- |
| `*.EMPRECSV` | `empresa` |
| `*.ESTABELE` | `estabelecimento` |
| `*.SOCIOCSV` | `socio` |
| `*.SIMPLES.CSV.*` | `simples` |
| `*.CNAECSV` | `cnae` |
| `*.MOTICSV` | `moti` |
| `*.MUNICCSV` | `munic` |
| `*.NATJUCSV` | `natju` |
| `*.PAISCSV` | `pais` |
| `*.QUALSCSV` | `quals` |

Durante a carga, o log registra o inicio e o fim da leitura de cada arquivo:

```text
empresa | INICIO LEITURA | K3241.K03200Y0.D60411.EMPRECSV
empresa | FIM LEITURA | K3241.K03200Y0.D60411.EMPRECSV | ... registros
```

## Fluxo Atual

1. Valida diretorios e arquivos de entrada.
2. Cria/verifica `rfb_import`, `dados_rfb_ops` e, na promocao, `dados_rfb`.
3. Cria tabelas raw em `rfb_import` sem indices/PK para carga rapida.
4. Carrega todos os arquivos RFB com `LOAD DATA LOCAL INFILE`.
5. Registra progresso, metricas, checkpoint e falhas em `dados_rfb_ops`.
6. Se a carga bruta terminar com sucesso, promove dados de `rfb_import` para `dados_rfb`.
7. Registra resultado da comparacao em `dados_rfb.controle_alteracao`.

## Tabelas Brutas

No banco `rfb_import` devem existir apenas tabelas de dados brutos:

```text
empresa
estabelecimento
socio
simples
cnae
moti
munic
natju
pais
quals
```

## Banco Operacional

O banco `dados_rfb_ops` guarda o controle de execucao:

```text
etl_execution
etl_run
etl_run_phase
etl_file_progress
etl_metric
etl_checkpoint
etl_dead_letter
data_quality_rule
```

## Banco Final

O banco `dados_rfb` contem as tabelas promovidas e a auditoria:

```text
empresa
estabelecimento
socio
simples
cnae
moti
munic
natju
pais
quals
controle_alteracao
```

Por padrao, a promocao usa `DB_PROMOTION_STRATEGY=rename_swap`: as tabelas carregadas em
`rfb_import` sao movidas para `dados_rfb` com `RENAME TABLE`, evitando copia linha-a-linha.
Depois disso, as tabelas brutas sao recriadas vazias em `rfb_import` para a proxima carga.

Antes do `rename_swap`, o ETL executa auditoria seletiva dos campos configurados em
`dados_rfb.controle_campo_monitorado`. Por padrao, `estabelecimento.situacao_cadastral`
e monitorado para registrar historico de mudanca ativa/inativa em
`dados_rfb.historico_campo_monitorado`. Essa auditoria so roda quando a tabela ja existe
em `dados_rfb`; se a tabela final ainda nao existe, a promocao e feita como copia simples
por `RENAME TABLE`, sem comparacao linha-a-linha.

A estrategia antiga de copia em lotes continua disponivel com `DB_PROMOTION_STRATEGY=copy`.
Nesse modo, se `dados_rfb` existir, compara tabela por tabela e registra em `controle_alteracao`:

- `sem alteracao`: assinatura da tabela nao mudou;
- `tem alteracao`: houve divergencia de contagem/checksum;
- `copiada`: tabela final nao existia e foi copiada.

Por performance, a auditoria detalhada campo-a-campo fica limitada por configuracao e por padrao roda apenas em tabelas pequenas de dominio.

## Variaveis Principais

```env
APP_VERSION=V3.0.0
DB_NAME=dados_rfb
IMPORT_DB_NAME=rfb_import
OPERATIONAL_DB_NAME=dados_rfb_ops

SYNC_STRATEGY=raw_import
LOAD_TARGET=final
LOAD_STRATEGY=load_data

IMPORT_DB_PER_RUN=False
RAW_IMPORT_RESET_TABLES=True
RAW_IMPORT_FAST_SCHEMA=True
PROMOTE_RAW_IMPORT_AFTER_LOAD=True
DB_PROMOTION_STRATEGY=rename_swap
MONITORED_FIELDS_BOOTSTRAP_DEFAULTS=True
CONTROL_DIFF_MAX_ROWS=1000
CONTROL_DIFF_DETAIL_TABLES=cnae,moti,munic,natju,pais,quals
DB_LOCAL_INFILE=True
```

`APP_VERSION` e exibida no cabecalho inicial do log para facilitar auditoria da versao
executada em producao.

Para `LOAD DATA LOCAL INFILE`, o MySQL/MariaDB tambem precisa estar com `local_infile=ON` no servidor.

## Execucao

```powershell
cd "F:\Repositorio\15_Git\RFB Loader Enterprise"
.\.venv\Scripts\Activate.ps1
python -m app.main
```

## Documentacao

- [Documentacao da aplicacao](docs/APPLICATION_DOCUMENTATION.md)
- [Requisitos](docs/REQUIREMENTS.md)
- [Roadmap v3](docs/V3_FEATURE_ROADMAP.md)
- [Diagramas Mermaid](docs/diagrams)
