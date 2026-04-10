# Ingestion Rollback Checklist

## When Rollback Is Mandatory
- el cierre live deja de estar en verde
- quality score o incident registry marcan degradacion critica
- curated serving queda inconsistente o ausente
- publication de snapshots queda incompleta
- capability registry deja de reflejar el scope promotable esperado

## Required Evidence To Preserve
- `docs/validation/operational/live/...`
- `docs/validation/operational/paper/...`
- `data/<env>/catalog/datasets.json`
- `data/<env>/catalog/dataset-contracts.json`
- `data/<env>/catalog/dataset-quality.json`
- `data/<env>/catalog/dataset-incidents.jsonl`
- `data/<env>/catalog/venue-capabilities.json`
- `data/<env>/catalog/delivery-contracts.json`
- `data/<env>/catalog/storage-lifecycle.json`
- `data/<env>/catalog/security-baseline.json`
- `data/<env>/serving/marketdata.sqlite`
- `data/<env>/publication/venue=BINANCE/stream_type=trade/events.jsonl`
- `data/<env>/publication/venue=BINANCE/stream_type=kline/events.jsonl`

## Rollback Procedure
1. Congelar la promotion o el cutover en curso.
2. Preservar todos los artefactos anteriores antes de regenerar nada.
3. Restaurar la baseline previa aprobada de ingestion.
4. Reejecutar `catalog-refresh`, `quality-refresh`, `curated-refresh` y `publish-snapshot` sobre la baseline restaurada.
5. Confirmar que el curated serving store y la publication quedan de nuevo coherentes con la baseline.
6. Abrir incidente con referencia a:
   - trigger del rollback
   - datasets afectados
   - quality/incidents afectados
   - operador y timestamp

## Retry Rules
- no reintentar promotion hasta tener un nuevo cierre paper/live en verde
- no reintentar mientras el catalogo o el serving store restaurado sigan inconsistentes
- no reintentar si publication no fue rehecha sobre la baseline restaurada

## Exit Criteria
- baseline previa activa
- catalogo coherente con la baseline restaurada
- serving store refrescado
- snapshots republicados
- artefactos del incidente preservados y trazables
