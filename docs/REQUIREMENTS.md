# Requisitos - RFB Loader Enterprise

## Objetivo Operacional

A aplicacao deve carregar os dados publicos de CNPJ da Receita Federal em MySQL/MariaDB com separacao entre:

- `rfb_import`: banco temporario de carga bruta dos arquivos CSV.
- `dados_rfb`: banco final consultavel.
- `rfb_ops`: banco operacional de execucao, metricas e checkpoints.

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
- Depois da promocao, as tabelas brutas devem ser recriadas vazias em `rfb_import` para a proxima execucao.
- A estrategia antiga de copia em lotes deve permanecer disponivel com `DB_PROMOTION_STRATEGY=copy`.
- A meta minima e reduzir em pelo menos 80% o tempo da promocao em comparacao com `INSERT INTO ... SELECT`.

## Requisitos De Auditoria Seletiva

- A aplicacao deve permitir configurar campos especificos para auditoria mensal.
- A configuracao deve ficar em `dados_rfb.controle_campo_monitorado`.
- Por padrao, `estabelecimento.situacao_cadastral` deve ser monitorado para registrar mudancas de ativa/inativa.
- A tabela de configuracao deve ser administrada apenas por usuarios administradores do banco.
- O ETL nao deve comparar todos os campos de todas as tabelas grandes.
- O ETL deve manter `dados_rfb.estado_campo_monitorado` como estado atual dos campos monitorados.
- O ETL deve registrar alteracoes em `dados_rfb.historico_campo_monitorado`, com tabela, campo, chave, valor anterior, valor novo, data e hora.
- A auditoria seletiva deve ocorrer antes do `rename_swap`, comparando o CSV novo em `rfb_import` contra o estado salvo da execucao anterior.

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
