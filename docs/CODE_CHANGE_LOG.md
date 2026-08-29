# Log De Alteracoes No Codigo

## 2026-06-18 - Monitoracao especifica de CNPJ e socios

### Contexto

A auditoria generica de campo monitorado em `estabelecimento.situacao_cadastral` era lenta para cargas grandes. A necessidade operacional passou a ser um conjunto fechado de perguntas: CNPJs novos, CNPJs ativados, CNPJs inativados e empresas com socios alterados.

### O Que Mudou

- Removida a configuracao antiga de auditoria generica por campo.
- A promocao `rename_swap` agora chama monitoracao especifica antes de trocar as tabelas, quando ja existe base anterior.
- Criadas as tabelas `monitoramento_cnpj_mudanca` e `resumo_monitoramento_cnpj`.
- A comparacao de estabelecimento usa `cnpj_basico`, `cnpj_ordem` e `cnpj_dv`.
- A comparacao de socios usa quantidade e hash dos dados por `cnpj_basico`.

### Para O Analista Junior

Depois da carga, consulte `dados_rfb.resumo_monitoramento_cnpj` para os totais e `dados_rfb.monitoramento_cnpj_mudanca` para os detalhes.
## 2026-06-07 - Versao da aplicacao no cabecalho do log

### Contexto

O cabecalho inicial do log mostrava o nome da aplicacao e o ambiente de producao, mas nao
identificava qual versao estava em execucao.

### O Que Mudou

- Em `app/config.py`, foi adicionada a configuracao `APP_VERSION`, lida do `.env`.
- Em `app/main.py`, o `banner()` agora registra `Versão da aplicação: <valor>`.
- Em `.env.example`, `README.md` e `docs/APPLICATION_DOCUMENTATION.md`, a variavel
  `APP_VERSION=V3.1.0` foi documentada.

### Para O Analista Junior

Quando precisar trocar a versao exibida no log, ajuste apenas a linha `APP_VERSION` no `.env`.
Na proxima execucao, o cabecalho inicial do log passara a mostrar a nova versao.

## 2026-06-07 - Promocao de tabelas novas sem comparacao linha-a-linha

### Contexto

O fluxo carrega os CSVs da Receita em `rfb_import` e depois promove as tabelas para
`dados_rfb`. Antes desta alteracao, a estrategia padrao `rename_swap` executava auditoria
de campos monitorados mesmo quando a tabela ainda nao existia no banco final. Nessa situacao
nao existe dado anterior para comparar, entao a varredura linha-a-linha nao traz ganho.

### O Que Mudou

- Em `app/etl/raw_import_promotion.py`, `_swap_all_tables_to_final()` agora verifica se a
  tabela ja existe em `dados_rfb` antes de chamar `_monitor_business_changes()`.
- Se a tabela nao existe em `dados_rfb`, o ETL registra no log que pulou comparacao/monitoracao
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
