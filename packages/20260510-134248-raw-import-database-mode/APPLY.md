# Pacote 20260510-134248-raw-import-database-mode

Objetivo: evitar `TRUNCATE`/lock no banco principal carregando CSV em banco de importação isolado.

## Aplicar

```powershell
cd "F:\Repositorio\15_Git\RFB Loader Enterprise"
git apply --3way "20260510-134248-raw-import-database-mode.patch"
.\venv\Scripts\python.exe -m compileall app
```

No Mac, o patch está em:

```text
/Users/vrelme/Documents/GitHub/DADOS_RFB/packages/20260510-134248-raw-import-database-mode/20260510-134248-raw-import-database-mode.patch
```

## Configuração recomendada no `.env` do Windows

```env
DB_NAME=dados_rfb
IMPORT_DB_NAME=dados_rfb_import
SYNC_STRATEGY=raw_import
LOAD_STRATEGY=load_data
LOAD_TARGET=final
MERGE_STRATEGY=full_refresh
DB_LOCAL_INFILE=True
ENABLE_PARALLELISM=True
ENABLE_ADAPTIVE_WORKERS=True
MIN_WORKERS=1
MAX_WORKERS=4
```

Com `SYNC_STRATEGY=raw_import`, a aplicação usa `IMPORT_DB_NAME` como banco ativo, cria o banco se não existir, carrega direto em `empresa`, `estabelecimento` e `socio` nesse banco de importação e não executa merge no banco principal.
