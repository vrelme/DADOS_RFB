# Dados Publicos CNPJ - RFB Loader Enterprise

Processo ETL para carga dos dados publicos do CNPJ disponibilizados pela Receita Federal do Brasil em MariaDB.

Fonte oficial e layout dos arquivos: [metadados da RFB](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

## Objetivo

A v3 separa a carga bruta dos arquivos, o controle operacional e a base final consultavel:

```text
rfb_import    -> carga bruta dos arquivos CSV da RFB
dados_rfb     -> base final promovida/consultavel
dados_rfb_ops -> controle operacional do ETL
```

Esse desenho evita misturar metadados do processo com dados de negocio e reduz bloqueios no banco final durante cargas grandes.

## Banco E Cliente SQL

Esta versao esta padronizada para:

- MariaDB Server, usando o driver Python `mysql+pymysql`;
- DBeaver 26.1.0 como cliente SQL recomendado para administracao, consultas e acompanhamento operacional.

Configuracao recomendada de conexao no DBeaver 26.1.0:

```text
Tipo de conexao: MariaDB
Host: localhost
Porta: 3306
Banco inicial: dados_rfb
Usuario: usuario configurado em DB_USER
Senha: senha configurada em DB_PASSWORD
```

Os bancos usados pela aplicacao sao criados/verificados pelo ETL conforme as variaveis `DB_NAME`,
`IMPORT_DB_NAME` e `OPERATIONAL_DB_NAME`.

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

Antes do `rename_swap`, quando ja existe base anterior em `dados_rfb`, o ETL executa uma
monitoracao especifica de negocio. Ela registra CNPJs novos, CNPJs que mudaram de ativo
para inativo, CNPJs que mudaram de inativo para ativo e empresas cujos socios mudaram.
Os detalhes ficam em `dados_rfb.monitoramento_cnpj_mudanca`; os totais ficam em
`dados_rfb.resumo_monitoramento_cnpj` e tambem em `dados_rfb.controle_alteracao`.

A estrategia antiga de copia em lotes continua disponivel com `DB_PROMOTION_STRATEGY=copy`.
Nesse modo, se `dados_rfb` existir, compara tabela por tabela e registra em `controle_alteracao`:

- `sem alteracao`: assinatura da tabela nao mudou;
- `tem alteracao`: houve divergencia de contagem/checksum;
- `copiada`: tabela final nao existia e foi copiada.

Por performance, a comparacao pesada fica restrita a perguntas de negocio especificas, evitando auditoria generica campo-a-campo em tabelas grandes.

## Variaveis Principais

```env
APP_VERSION=V3.1.0
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
CONTROL_DIFF_MAX_ROWS=1000
CONTROL_DIFF_DETAIL_TABLES=cnae,moti,munic,natju,pais,quals
DB_LOCAL_INFILE=True
```

`APP_VERSION` e exibida no cabecalho inicial do log para facilitar auditoria da versao
executada em producao.

Para `LOAD DATA LOCAL INFILE`, o MariaDB tambem precisa estar com `local_infile=ON` no servidor.

Validacao recomendada no DBeaver 26.1.0:

```sql
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;
```

## Execucao

```powershell
cd "F:\Repositorio\15_Git\RFB Loader Enterprise"
.\.venv\Scripts\Activate.ps1
python -m app.main
```

## Observabilidade, Prometheus E Grafana

A aplicacao de metricas roda separada do ETL principal e expoe endpoints para Prometheus e
para paineis JSON:

```powershell
uvicorn app.observability.metrics_app:app --host 0.0.0.0 --port 8000
```

Endpoints disponiveis:

```text
GET /health
GET /metrics
GET /dashboard/overview
GET /dashboard/performance
GET /dashboard/database
GET /dashboard/execution
GET /dashboard/promotion
GET /dashboard/audit
```

Configure o intervalo de coleta no `.env`:

```env
METRICS_COLLECTION_INTERVAL=30
```

Stack local:

```powershell
docker compose up -d prometheus grafana node-exporter mysql-exporter rfb-metrics-api
```

URLs padrao:

```text
Metrics API: http://localhost:8000
Prometheus:  http://localhost:9090
Grafana:     http://localhost:3000
```

No Grafana, cadastre o Prometheus em `http://prometheus:9090`.

### Dashboard 1 - Visao Geral

Cards sugeridos:

```promql
rfb_etl_last_execution_timestamp
rfb_etl_last_execution_status
rfb_etl_last_execution_duration_seconds
rfb_etl_files_processed_total
rfb_etl_records_processed_total
rfb_etl_errors_total
```

### Dashboard 2 - Performance

Graficos sugeridos:

```promql
rfb_etl_records_per_second
rfb_etl_file_duration_seconds
rfb_etl_stage_duration_seconds
rfb_etl_merge_duration_seconds
rfb_etl_rename_swap_duration_seconds
```

Labels principais: `pipeline`, `file_name`, `table_name`, `stage`, `status`.

### Dashboard 3 - Banco De Dados

Cards sugeridos:

```promql
rfb_db_empresa_total
rfb_db_estabelecimento_total
rfb_db_socio_total
rfb_db_cnae_total
rfb_db_municipio_total
```

### Dashboard 4 - Execucao

Tabela e series por arquivo:

```promql
rfb_etl_execution_total
rfb_etl_execution_duration_seconds
rfb_etl_execution_records_total
rfb_etl_execution_status_total
```

Labels principais: `pipeline`, `worker`, `file_name`, `status`, `table_name`.

### Dashboard 5 - Promotion

Promocoes e rename swap:

```promql
rfb_promotion_total
rfb_promotion_rename_swap_total
rfb_promotion_duration_seconds
rfb_promotion_status_total
```

Labels principais: `table_name`, `strategy`, `status`.

### Dashboard 6 - Auditoria

Eventos de campos monitorados:

```promql
rfb_audit_fields_added_total
rfb_audit_fields_removed_total
rfb_audit_fields_changed_total
rfb_audit_events_total
```

Labels principais: `table_name`, `field_name`, `event_type`.

### Dashboard 7 - Infraestrutura

Via Node Exporter:

```promql
100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))
100 * (1 - (node_filesystem_avail_bytes / node_filesystem_size_bytes))
rate(node_network_receive_bytes_total[5m])
rate(node_network_transmit_bytes_total[5m])
```

Metricas esperadas:

```text
node_cpu_seconds_total
node_memory_MemAvailable_bytes
node_filesystem_avail_bytes
node_network_receive_bytes_total
node_network_transmit_bytes_total
```

### Dashboard 8 - MySQL

Via MySQL Exporter:

```promql
rate(mysql_global_status_queries[5m])
mysql_global_status_threads_connected
rate(mysql_global_status_innodb_row_lock_waits[5m])
1 - (
  rate(mysql_global_status_innodb_buffer_pool_reads[5m])
  /
  rate(mysql_global_status_innodb_buffer_pool_read_requests[5m])
)
rate(mysql_global_status_bytes_received[5m])
rate(mysql_global_status_bytes_sent[5m])
```

Metricas esperadas:

```text
mysql_global_status_queries
mysql_global_status_threads_connected
mysql_global_status_innodb_row_lock_waits
mysql_global_status_innodb_buffer_pool_reads
mysql_global_status_innodb_buffer_pool_read_requests
mysql_global_status_bytes_received
mysql_global_status_bytes_sent
```

## Documentacao

- [Documentacao da aplicacao](docs/APPLICATION_DOCUMENTATION.md)
- [Requisitos](docs/REQUIREMENTS.md)
- [Roadmap v3](docs/V3_FEATURE_ROADMAP.md)
- [Diagramas Mermaid](docs/diagrams)
