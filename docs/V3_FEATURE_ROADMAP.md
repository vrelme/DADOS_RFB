# Versao 3 - Plano de Fechamento

## Decisao Arquitetural

A v3 deve separar ingestao bruta e tratamento:

- `rfb_import`: carga bruta dos CSVs, sem bloquear o banco principal.
- `dados_rfb`: base oficial/tratada, atualizada por etapa posterior de qualidade/sincronizacao.

Esse desenho evita `TRUNCATE` em tabelas finais em uso e reduz lock contention no MariaDB.

## Entregue Nesta Fundacao


### Progresso Operacional / ETA

Base criada via tabelas:

- `etl_run`
- `etl_run_phase`
- `etl_file_progress`

Essas tabelas permitem acompanhar:

- status global da execução;
- fase atual;
- tabela atual;
- arquivo atual;
- total de arquivos;
- arquivos concluídos;
- arquivos com falha;
- registros processados;
- percentual de progresso;
- estimativa de término (`estimated_finish_at`);
- heartbeat da execução.

Consulta operacional base:

```sql
SELECT
    id,
    status,
    current_phase,
    current_table,
    current_file,
    total_files,
    completed_files,
    failed_files,
    progress_percent,
    total_records,
    started_at,
    heartbeat_at,
    estimated_finish_at
FROM etl_run
ORDER BY id DESC
LIMIT 1;
```

### Grafana Dashboard

Base criada via tabela `etl_metric`.

Metricas iniciais:

- `records_processed`
- `execution_seconds`
- `chunk_records_processed`

Consultas base para Grafana:

```sql
SELECT created_at AS time, metric_value, table_name, file_name
FROM etl_metric
WHERE metric_name = 'records_processed'
ORDER BY created_at;
```

```sql
SELECT created_at AS time, metric_value, table_name
FROM etl_metric
WHERE metric_name = 'execution_seconds'
ORDER BY created_at;
```

### Checkpoint Resume

Base criada via tabela `etl_checkpoint`.

Quando `ENABLE_CHECKPOINT_RESUME=True`, arquivo com checkpoint `SUCCESS` nao e reprocessado.

### Dead Letter Queue

Base criada via tabela `etl_dead_letter`.

A proxima etapa e conectar as regras de validacao para enviar registros invalidos para essa tabela em vez de descartar.

### Data Quality Rules Engine

Base criada via tabela `data_quality_rule`.

Campos planejados:

- `table_name`
- `column_name`
- `rule_type`
- `rule_expression`
- `severity`
- `enabled`

### Compressao Inteligente

`ENABLE_ZIP_PROCESSING=True` faz o ETL extrair `.zip` de `INPUT_DIR` para `EXTRACT_DIR` e descobrir arquivos extraidos automaticamente.


## Atualizacao Implementada - Carga RFB Completa

A v3 passou a carregar todos os grupos de arquivos descompactados da Receita Federal em `rfb_import`:

- `empresa`;
- `estabelecimento`;
- `socio`;
- `simples`;
- `cnae`;
- `moti`;
- `munic`;
- `natju`;
- `pais`;
- `quals`.

A carga usa `RFB_TABLES` como manifesto unico de padroes, colunas e chaves. O fluxo principal e:

```text
Arquivos RFB -> rfb_import -> dados_rfb -> controle_alteracao
                 |
                 v
             dados_rfb_ops
```

Decisoes implementadas:

- `rfb_import` fica somente com dados brutos;
- `dados_rfb_ops` guarda metadados operacionais;
- `dados_rfb` recebe a copia/promocao final;
- `controle_alteracao` registra se houve ou nao divergencia por tabela;
- detalhes campo-a-campo sao limitados por `CONTROL_DIFF_DETAIL_TABLES` e `CONTROL_DIFF_MAX_ROWS`.

## Proximas Implementacoes

### Retry Inteligente

Implementar fila de reprocessamento baseada em `etl_checkpoint.status = FAILED` e `etl_execution.retry_count`.

### DLQ Real

Alterar `Validator` para retornar registros validos e invalidos. Invalidos devem ser persistidos em `etl_dead_letter` com motivo e payload bruto.

### Rules Engine

Implementar interpretador para regras em `data_quality_rule`, com operadores como:

- `required`
- `length`
- `regex`
- `in`
- `numeric`


### Async Logging

Objetivo: evitar gargalo de I/O causado por escrita síncrona de logs durante cargas grandes.

Entregáveis:

- substituir handlers diretos por `QueueHandler` e `QueueListener`;
- manter rotação de arquivo e saída console;
- garantir flush controlado no encerramento do ETL;
- registrar perdas de log apenas em situação de erro crítico.

Benefícios:

- menor bloqueio por disco;
- maior throughput durante `LOAD DATA` e validação;
- logs mais previsíveis em execução paralela.

### Particionamento MariaDB

Objetivo: preparar tabelas gigantes para consultas e manutenção em escala.

Estratégias candidatas:

- `empresa`: particionamento por hash de `cnpj_basico`;
- `estabelecimento`: particionamento por `uf` ou hash de CNPJ completo;
- `socio`: particionamento por hash de `cnpj_basico`;
- tabelas históricas/logs: particionamento por data.

Entregáveis:

- scripts SQL versionados em `database/partitions/`;
- avaliação de impacto em primary keys e unique keys do MariaDB;
- documentação de manutenção de partições;
- testes de carga comparando tabela particionada e não particionada.

Benefícios:

- consultas mais rápidas por filtro de UF/data/hash;
- manutenção simplificada;
- menor impacto em purge/archive;
- merges e sincronizações mais previsíveis.

### Arquivamento Automático

Objetivo: mover execuções, métricas e DLQ antigas para tabelas/banco de arquivo.

Entregáveis:

- configuração `ARCHIVE_RETENTION_DAYS`;
- tabelas de arquivo como `etl_execution_archive`, `etl_metric_archive`, `etl_dead_letter_archive`;
- job de arquivamento seguro;
- métricas de quantidade arquivada e tempo de execução.

Benefícios:

- banco operacional menor;
- dashboards mais rápidos;
- menor custo de backup e manutenção.

### API Operacional ETL

Objetivo: expor status e comandos operacionais via API REST.

Endpoints planejados:

- `GET /status`: estado atual do ETL;
- `GET /metrics`: métricas recentes;
- `GET /executions`: histórico de execuções;
- `GET /executions/{id}`: detalhe de uma execução;
- `POST /retry`: reprocessar falhas elegíveis;
- `POST /stop`: solicitar parada controlada;
- `POST /start`: iniciar ETL com parâmetros controlados.

Entregáveis:

- aplicação FastAPI separada do worker ETL;
- autenticação simples por token no primeiro momento;
- contratos JSON estáveis;
- documentação OpenAPI.

Benefícios:

- integração externa;
- automação operacional;
- base para dashboard web e Grafana annotations.

### Web Admin ETL

Objetivo: fornecer painel operacional para equipe acompanhar e controlar o ETL.

Funções planejadas:

- iniciar ETL;
- solicitar parada segura;
- acompanhar progresso por tabela/arquivo;
- visualizar falhas e DLQ;
- executar retry;
- consultar throughput, workers e tempos.

Entregáveis:

- frontend administrativo simples;
- integração com API Operacional;
- tela de execuções;
- tela de falhas/retry;
- tela de métricas operacionais.

Benefícios:

- operação sem acesso direto ao servidor;
- menor dependência de terminal;
- troubleshooting mais rápido.

### Testes Automatizados Reais

Objetivo: estabilizar a v3 antes de evoluir integrações enterprise.

Categorias:

- unitários: validação, regras, DLQ, checkpoints;
- integração: MariaDB local/test container, criação de schema, carga pequena;
- carga: arquivos sintéticos grandes;
- concorrência: paralelismo, retry, lock handling;
- regressão: garantir que `raw_import` não toque o banco principal.

Entregáveis:

- suíte `pytest`;
- fixtures de CSV pequenos;
- testes de banco isolado;
- relatório de cobertura mínimo;
- execução local e em CI.

Benefícios:

- redução de regressões;
- validação antes de deploy;
- segurança para refatorar carga, sync e observabilidade.

### CI/CD Completo

Objetivo: automatizar validação, build e deploy.

Pipeline planejado:

1. `git push`;
2. lint/format check;
3. testes unitários;
4. testes de integração com banco;
5. build de imagem Docker;
6. publicação de artefato;
7. deploy controlado.

Entregáveis:

- workflow GitHub Actions;
- Dockerfile para ETL;
- docker-compose para ambiente local;
- secrets por ambiente;
- estratégia de release/tag.

Benefícios:

- versionamento confiável;
- deploy reproduzível;
- menor risco operacional.

### Streaming Loader

Implementar modo `STREAMING_LOADER=True` para monitorar pasta continuamente e processar novos arquivos.

### Airflow Integration

Criar DAG separada para:

1. baixar/extrair arquivos;
2. carregar `rfb_import`;
3. validar qualidade;
4. sincronizar `dados_rfb`;
5. emitir metricas finais.

### OpenTelemetry Tracing

Adicionar spans para:

- discovery/extracao zip;
- load data por arquivo;
- merge/sync;
- validacao;
- DLQ.


## Ordem Recomendada Para Fechar a v3

1. `raw_import` estável sem lock no banco principal.
2. DLQ real e Data Quality Rules Engine.
3. Checkpoint Resume e Retry Inteligente completos.
4. Grafana Dashboard com métricas reais.
5. Async Logging.
6. Testes automatizados reais.
7. API Operacional ETL.
8. Web Admin ETL.
9. Arquivamento automático.
10. Particionamento MariaDB validado por benchmark.
11. Airflow Integration.
12. OpenTelemetry Tracing.
13. CI/CD completo com Docker e deploy.

Critério de fechamento da v3: carga bruta em `rfb_import`, validação com DLQ, checkpoint/retry, métricas consultáveis no Grafana e testes automatizados mínimos passando.
