# Prompt de arranque (pegable en Claude Code)

Trabajo en el repo:
- Root: `C:\\Users\\oortega\\OneDrive - BOARD\\Documents\\Projects\\MyTraderSystem`

Reglas:
- No hacer `git commit`/`git push` sin petición explícita.
- Preferir Docker como entorno de referencia (Python 3.11): `make docker-up`, `make docker-exec`.
- Tras cambios en `docs-html/`, ejecutar siempre `python scripts/docs_search_sync.py --backend static --mode incremental --docs-root docs-html`.
- No tocar `data/` ni `errors/` salvo tareas explícitas: son artefactos de runtime.

Comandos canónicos:
- Instalar: `make install` (o Docker)
- Lint: `make lint`
- Tests fast: `make test` / Docker: `make docker-test`
- Tests full: `make test-all` / Docker: `make docker-test-all`
- Levantar control plane: `make controlplane-web` y `make controlplane-worker`

Runbooks (fuentes de verdad):
- paper: `docs/operations/ingestion/ingestion_operational_closure_paper.md`
- live: `docs/operations/ingestion/ingestion_operational_closure_live.md`
- happy paths: `docs/operations/application/*_happy_path_runbook.md`

