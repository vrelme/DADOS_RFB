# Requisitos - RFB Loader Enterprise

## Objetivo Operacional

A aplicacao deve carregar os dados publicos de CNPJ da Receita Federal em MariaDB com separacao entre:

- `rfb_import`: banco temporario de carga bruta dos arquivos CSV.
- `dados_rfb`: banco final consultavel.
- `dados_rfb_ops`: banco operacional de execucao, metricas e checkpoints.

O cliente SQL recomendado para administracao e acompanhamento operacional e o DBeaver 26.1.0, configurado com conexao do tipo MariaDB.

## Requisitos De Carga

- A carga dos arquivos deve usar `LOAD DATA LOCAL INFILE` como estrategia principal.
- Os arquivos devem ser descobertos pelos padroes definidos em `app/etl/rfb_manifest.py`.
- A carga deve registrar inicio, fim, quantidade de registros e taxa de processamento por arquivo.
- Quando `RAW_IMPORT_RESET_SCHEMA=True`, checkpoints antigos nao podem impedir a releitura dos CSVs.
- Antes da promocao, o ETL deve validar que toda tabela com arquivo encontrado possui registros em `rfb_import`.

## Requisitos De Promocao

- A promocao de `rfb_import` para `dados_rfb` deve evitar copia linha-a-linha em tabelas grandes.
- A estrategia padrao deve ser `DB_PROMOTION_STRATEGY=rename_swap`.
- A promocao por `rename_swap` deve mover as tabelas carregadas de `rfb_import` para `dados_rfb` usando `RENAME TABLE`.
- Quando a tabela final nao existir em `dados_rfb`, o ETL nao deve fazer comparacao/auditoria linha-a-linha; deve promover a tabela nova diretamente.
- Quando a tabela final ja existir em `dados_rfb`, o ETL deve registrar divergencias configuradas antes de substituir a tabela pelo dado mais recente de `rfb_import`.
- Depois da promocao, as tabelas brutas devem ser recriadas vazias em `rfb_import` para a proxima execucao.
- A estrategia antiga de copia em lotes deve permanecer disponivel com `DB_PROMOTION_STRATEGY=copy`.
- A meta minima e reduzir em pelo menos 80% o tempo da promocao em comparacao com `INSERT INTO ... SELECT`.

## Requisitos De Monitoracao De CNPJ E Socios

- O ETL deve registrar CNPJs novos antes do `rename_swap`, comparando `rfb_import.estabelecimento` com `dados_rfb.estabelecimento`.
- O ETL deve registrar CNPJs que ficaram ativos, considerando mudanca de situacao cadastral para `02`.
- O ETL deve registrar CNPJs que ficaram inativos, considerando mudanca de `02` para qualquer outra situacao cadastral.
- O ETL deve registrar empresas cujos socios mudaram, comparando quantidade e hash dos dados de socios por `cnpj_basico`.
- Os detalhes devem ser gravados em `dados_rfb.monitoramento_cnpj_mudanca`.
- Os totais por execucao devem ser gravados em `dados_rfb.resumo_monitoramento_cnpj` e tambem resumidos em `dados_rfb.controle_alteracao`.
- A monitoracao deve ocorrer antes do `rename_swap` apenas para tabelas ja existentes no banco final, comparando o CSV novo em `rfb_import` contra a base anterior em `dados_rfb`.
## Requisitos De Tempo E Observabilidade

- O log deve mostrar o tempo gasto por tabela durante a carga dos CSVs.
- O log deve mostrar o tempo gasto por tabela durante a promocao para `dados_rfb`.
- O log deve mostrar o tempo total de carga, tempo total de promocao e tempo total do pipeline.
- O bloco final `RESUMO DE TEMPOS` deve consolidar esses valores.

## Requisitos De Documentacao

- Toda alteracao funcional relevante deve atualizar a documentacao da aplicacao na mesma branch.
- Mudancas de estrategia de carga, promocao, variaveis `.env`, checkpoints, validacoes e performance devem ser documentadas antes de fechar a versao.
- O `README.md` deve refletir o fluxo recomendado para operadores.
- `docs/APPLICATION_DOCUMENTATION.md` deve refletir o comportamento tecnico implementado.
