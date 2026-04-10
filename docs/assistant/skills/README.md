# Skills (Playbooks) portados desde Codex

Este directorio copia los `SKILL.md` que vivían fuera del repo en `C:\\Users\\oortega\\.codex\\skills\\...` para que Claude Code pueda consumirlos como playbooks versionados.

## Cómo usarlos (Claude Code)
Regla: trata cada skill como un procedimiento operativo/documental, no como “magia”.

Workflow recomendado:
1. Abrir el `SKILL.md` del playbook que aplique.
2. Seguir sus reglas (especialmente las de rutas reales, incrementalidad y no invención).
3. Ejecutar los comandos canónicos del repo si el playbook lo requiere.
4. Si el playbook toca `docs-html/`, ejecutar siempre:
   - `python scripts/docs_search_sync.py --backend static --mode incremental --docs-root docs-html`

## Skills mínimos (continuidad ingestion + docs)
- `actualizacion-general-docs/`
- `auditoria-ingestion/`
- `plan-remediacion-ingestion/`
- `backlog-ingestion/`

## Skills opcionales (feature + docs)
- `auditoria-docs/`
- `auditoria-feature/`
- `plan-remediacion-feature/`
- `backlog-feature/`
- `iteracion-feature/`

## Nota
Estos playbooks se copian tal cual para mantener continuidad. Si se actualizan, hacerlo dentro del repo para que queden versionados.

