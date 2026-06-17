# Release V3.1.0

## Resumo

Melhoria de monitoramento do processo de auditoria de campos monitorados, com:

- exibição de progressão de lotes no formato `lote X/Y`
- cálculo de ETA por campo monitorado
- resumo final com total de alterações, tempo e throughput de registros por segundo
- redução do custo de monitoramento com contagem de lotes calculada uma vez por campo
- ajuste de versão para `V3.1.0`

## O que mudou

### `app/etl/raw_import_promotion.py`

- adicionada função `_format_duration` para exibir tempo em `s`, `m:ss` e `h:mm:ss`
- calculado `total_batches` com `math.ceil(total_rows / batch_size)`
- registrado `lote {batch_count}/{total_batches}` em cada progresso de lote
- exibido `ETA` aproximado com base em tempo médio de lote
- exibido resumo final com `tempo` e `velocidade` em `reg/s`

### Versão

- `app/config.py`: valor padrão de `APP_VERSION` atualizado para `V3.1.0`
- `.env`, `.env.example`, `README.md` e `docs/APPLICATION_DOCUMENTATION.md`: versão atualizada para `V3.1.0`
- `docs/CODE_CHANGE_LOG.md`: documentação de versão atualizada para `V3.1.0`

## Como validar

1. Ajustar `APP_VERSION=V3.1.0` no `.env` ou usar o `.env.example`
2. Executar o ETL normalmente com auditoria de campos monitorados ativada
3. Conferir nos logs as linhas de progresso no formato:
   - `lote 18/99`
   - `ETA: 2m15s`
   - `total=... alteracoes em ... lotes | tempo=... | velocidade=... reg/s`

## Observações

Essa melhoria é uma evolução de `V3.0.0` e foi formalizada como `V3.1.0`.
