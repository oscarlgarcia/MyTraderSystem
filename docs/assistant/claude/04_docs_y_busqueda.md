# Documentación HTML y Búsqueda

## Regla operativa (obligatoria)
Tras cualquier cambio material en `docs-html/` (nuevas páginas, navegación, secciones, assets), ejecutar:

`python scripts/docs_search_sync.py --backend static --mode incremental --docs-root docs-html`

Esto actualiza normalmente:
- `docs-html/assets/search-index.json`
- `docs-html/search.html`
(y a veces `docs-html/assets/styles.css`, `docs-html/assets/app.js`, `docs-html/assets/icons.svg`)

## Nota sobre ruta canónica vs ruta física
Hay una discrepancia conocida:
- ruta canónica de publicación: `docs/docs-html/`
- ruta física en este workspace: `docs-html/`

La documentación refleja este hueco en “Nota editorial”. No asumir que es un bug del contenido.

