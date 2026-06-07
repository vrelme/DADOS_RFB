# Log De Alteracoes No Codigo

## 2026-06-07 - Promocao de tabelas novas sem comparacao linha-a-linha

### Contexto

O fluxo carrega os CSVs da Receita em `rfb_import` e depois promove as tabelas para
`dados_rfb`. Antes desta alteracao, a estrategia padrao `rename_swap` executava auditoria
de campos monitorados mesmo quando a tabela ainda nao existia no banco final. Nessa situacao
nao existe dado anterior para comparar, entao a varredura linha-a-linha nao traz ganho.

### O Que Mudou

- Em `app/etl/raw_import_promotion.py`, `_swap_all_tables_to_final()` agora verifica se a
  tabela ja existe em `dados_rfb` antes de chamar `_audit_monitored_fields()`.
- Se a tabela nao existe em `dados_rfb`, o ETL registra no log que pulou comparacao/auditoria
  e promove a tabela diretamente com `RENAME TABLE`.
- Se a tabela existe em `dados_rfb`, o comportamento anterior permanece: audita campos
  monitorados configurados e depois troca a tabela final pelo dado mais novo de `rfb_import`.
- Em `_compare_or_copy_table()`, o modo antigo de copia em lotes agora tambem registra
  explicitamente no log que tabelas finais inexistentes sao copiadas sem comparacao.

### Por Que Isso E Correto

Para comparar alteracoes, e preciso existir uma versao anterior da tabela no banco final.
Quando a tabela ainda nao existe, todo registro vindo de `rfb_import` e necessariamente novo.
Nesse caso, a operacao correta e criar/promover a tabela final e registrar `status=copiada`.

### Como Validar

Execute:

```bash
pytest tests/test_raw_import_promotion.py
```

O teste simula dois cenarios:

- tabela final inexistente: auditoria nao e chamada e o status registrado e `copiada`;
- tabela final existente: auditoria e chamada e o status registrado e `promovida`.
