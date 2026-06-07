# Release Notes — v3.0.0

Data: 2026-06-07
Comparação: `v2.0.0` → `feature/v3-base` (HEAD)

## Sumário rápido
- Nova estratégia de promoção `rename_swap` documentada e suportada (RENAME duplo, descarte de backup e recriação das tabelas em `rfb_import`).
- Refatoração / novo código ETL: novos repositórios e reorganização em `app/etl/` (`raw_import_promotion.py`, `merge_repository.py`, `observability_repository.py`, `etl_execution_repository.py`, entre outros).
- Documentação ampliada: `docs/` contendo `APPLICATION_DOCUMENTATION.md`, `REQUIREMENTS.md` atualizado, diagramas Mermaid (architecture, etl-flow, sequence-load, ERD).
- Mudança na organização de dependências: `requirements/` com `base.txt` e `dev.txt` (arquivo `requirements.txt` removido).
- Scripts e utilitários: `create_table.py`, `quick_check.py`, `verify_empresa_data.py`, `simple_empresa_check.py` adicionados.
- Remoção de artefatos desnecessários (`code/` legacy scripts removidos, arquivos de `venv` e caches não versionados foram limpos).

## Novidades (detalhado)
- Promoção por `rename_swap` (alta performance):
  - Quando a tabela final já existe: RENAME duplo (final → `final__previous`; `rfb_import.table` → final) e, em seguida, `DROP TABLE IF EXISTS final__previous`.
  - Quando a tabela final não existe: RENAME simples do import para final.
  - Após o swap, a tabela bruta em `rfb_import` é recriada vazia (DDL gerado por `raw_import_create_table_sql(...)`).
  - Auditoria seletiva de campos é executada antes do swap; resultados gravados em `dados_rfb.historico_campo_monitorado` e `dados_rfb.estado_campo_monitorado`.
  - Inserção de registro em `dados_rfb.controle_alteracao` para cada tabela promovida (status `promovida`/`copiada`).

- Repositórios e componentes novos/alterados em `app/etl/`:
  - `raw_import_promotion.py`: implementação do fluxo de promoção (`rename_swap` e `copy` strategies).
  - `merge_repository.py`: novo arquivo para gerenciar merges/ops na promoção (adicionado).
  - `observability_repository.py` e `etl_execution_repository.py`: integram métricas, checkpoints e execução operacional.
  - `bulk_repository.py`, `deduplicator.py`, `etl_execution_repository.py` atualizados para suportar o novo fluxo.

- Documentação e diagramas:
  - `docs/APPLICATION_DOCUMENTATION.md` (novo): documentação técnica extensa sobre arquitetura, fluxo ETL, auditoria, promoção e operação.
  - `docs/REQUIREMENTS.md` (atualizado) com as regras de promoção e observabilidade.
  - Novos diagramas em `docs/diagrams/` (architecture, etl-flow, sequence-load, erd, etc.).

- Organização de dependências e ambiente:
  - `requirements.txt` removido; agora em `requirements/base.txt` e `requirements/dev.txt`.
  - Observação: `venv` arquivos foram removidos do controle de versão (limpeza).

## Mudanças que podem impactar operadores / breaking changes
- `rfb_import` não mantém os dados após promoção — as tabelas brutas são recriadas vazias. Operadores que esperavam encontrar os dados brutos preservados devem ajustar processos ou configurar backups antes da promoção.
- A estratégia `DB_PROMOTION_STRATEGY` agora documentada como `rename_swap` por padrão; verifique `.env` e `app/config.py` antes de atualização.
- Se você dependia do arquivo `requirements.txt`, atualize seus scripts/CI para instalar a partir de `requirements/base.txt` e `requirements/dev.txt`.

## Arquivos-chave adicionados/modificados
- Alterados (exemplos relevantes):
  - `README.md` (nota operacional sobre `rename_swap`)
  - `app/etl/raw_import_promotion.py` (novo/adição significativa)
  - `app/etl/merge_repository.py` (novo)
  - `app/etl/observability_repository.py` (novo)
  - `app/etl/etl_execution_repository.py` (novo)
  - `app/config.py`, `app/database.py`, `app/orchestrator.py`, `app/models.py` (ajustes operacionais)
  - `docs/REQUIREMENTS.md`, `docs/APPLICATION_DOCUMENTATION.md`, `docs/V3_FEATURE_ROADMAP.md` (documentação)
  - `docs/diagrams/*` (novos diagramas mermaid)
  - `create_table.py`, `quick_check.py`, `verify_empresa_data.py` (scripts utilitários)
  - `requirements/base.txt`, `requirements/dev.txt` (novos)

- Removidos / limpos (não exaustivo):
  - `code/*` (diversos scripts legacy removidos)
  - `requirements.txt` (substituído)
  - arquivos de ambiente/venv e caches (`venv/*`, `app/__pycache__/*`) — limpeza de artefatos.

## Commits (resumo)
Ver o log de commits entre `v2.0.0` e HEAD para histórico detalhado de cada mudança.

## Passos de migração / checklist para operações
1. Fazer backup completo do banco `dados_rfb` e, se desejar, do schema `rfb_import` antes da primeira execução da nova versão.
2. Atualizar o ambiente `.env` conforme as variáveis em `README.md` e `app/config.py` (ex.: `DB_PROMOTION_STRATEGY=rename_swap`, `PROMOTE_RAW_IMPORT_AFTER_LOAD=True`).
3. Instalar dependências a partir de `requirements/base.txt` e `requirements/dev.txt` (em CI, usar `pip install -r requirements/base.txt`).
4. Revisar e aplicar permissões de banco recomendadas para `controle_campo_monitorado` (veja `docs/APPLICATION_DOCUMENTATION.md`).
5. Executar um teste em ambiente de staging: carregar pequenos CSVs, confirmar promotion (`dados_rfb.<table>` tem dados), `rfb_import.<table>` existe e está vazia, e `dados_rfb.controle_alteracao` tem registros.
6. Se desejar manter backups mais longos do `final__previous`, alterar o fluxo antes de habilitar em produção (atualmente o backup é descartado imediatamente).

## Testes e QA
- Novos artefatos de teste minimal: `tests/test_logger.py` adicionado; recomenda-se expandir cobertura com testes de integração para `raw_import_promotion`.
- Recomenda-se um test-run end-to-end em staging antes de promover em produção.

## Sugestões futuras
- Adicionar testes de integração para garantir sequência RENAME duplo + DROP + recriação do import.
- Opção de retenção configurável para backups `final__previous` (retenção por N execuções ou por período configurável).
- Suporte a uma estratégia de arquivamento automático (copiar `rfb_import` para `dados_rfb_archive` antes do drop).

---

Arquivo gerado automaticamente a partir do diff entre `v2.0.0` e a branch atual (`feature/v3-base`).
