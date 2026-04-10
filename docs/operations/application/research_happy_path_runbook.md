# Research Happy Path Runbook

## Objetivo
Ejecutar el happy path minimo para investigar la aplicacion y sus features sin activar paper/live.

## Comando principal

```powershell
python -m app --env dev --mode dry --max-events 200 --trace-steps
```

## Uso recomendado
- smoke de estrategia
- inspeccion del flujo `market event -> features -> strategy -> signal`
- validacion rapida de wiring antes de abrir backtesting o paper

## Complementos utiles
- consultar catalogo de datasets:

```powershell
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8000/api/datasets/catalog'
```

- consultar quality:

```powershell
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8000/api/datasets/quality'
```
