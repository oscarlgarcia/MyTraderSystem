---
name: auditoria-feature
description: Audita modulos de Feature Store y Feature Engine para sistemas cuantitativos de trading. Usa esta skill cuando se necesite una revision tecnica rigurosa, basada en codigo real, sobre definicion de features, consistencia temporal, leakage, calculo incremental, versionado, serving online/offline, reproducibilidad, storage por entidad+timestamp, integracion con modelos o estrategias, y readiness para research, backtesting, paper trading o live trading.
---

# Auditoria Feature

## Objetivo

Emitir una auditoria tecnica dura, basada en evidencia de codigo real, para decidir si un modulo de Feature Store + Feature Engine sirve solo para research exploratorio o puede sostener:

- research reproducible
- backtesting serio
- entrenamiento reproducible
- paper trading
- live trading

La auditoria debe centrarse en leakage, causalidad temporal, consistencia online/offline, calculo incremental, versionado, serving y contrato con modelos o estrategias.

## Regla principal

Inspeccionar siempre el codigo real antes de concluir.

No asumir funcionalidades no demostradas.

Si algo no puede demostrarse desde:

- codigo
- configuracion
- tests
- scripts
- artifacts
- documentacion versionada

marcarlo explicitamente como `NO VERIFICABLE CON LA EVIDENCIA ACTUAL`.

## Flujo obligatorio

1. Inspeccionar el modulo completo relevante:
   - `app/features`
   - contratos de eventos / features / labels / signals
   - storage / cache / registry / serving
   - integracion con estrategias, modelos o pipelines
   - configuracion
   - tests
   - documentacion tecnica
2. Identificar el scope real soportado hoy:
   - definicion de features
   - calculo incremental
   - persistencia
   - serving
   - versionado
   - reproducibilidad
3. Separar con rigor:
   - lo implementado
   - lo parcial
   - lo ausente
   - lo no verificable
4. Evaluar el sistema como infraestructura cuantitativa, no como libreria de indicadores.
5. Emitir veredicto por nivel de uso:
   - research
   - backtesting
   - entrenamiento reproducible
   - paper
   - live

## Reglas de evaluacion

Aplicar siempre estas reglas:

- No confundir "calcular indicadores" con "tener un Feature Engine serio".
- No confundir "guardar vectores en JSON/Parquet" con "tener una Feature Store real".
- No confundir "rolling window" con "no leakage".
- No confundir "mismo codigo offline/online" con "consistencia demostrada".
- No confundir "version string" con "versionado reproducible".
- No confundir "cache" con "serving online serio".

## Criterios tecnicos obligatorios a inspeccionar

### 1. Definicion de features

Verificar:

- abstraccion explicita de feature definitions
- metadata por feature
- facilidad de registro/extensibilidad
- catalogo o registry real
- dependencias entre features
- versionado de definiciones

### 2. Consistencia temporal y causalidad

Verificar:

- uso exclusivo de informacion disponible hasta el timestamp de decision
- leakage / look-ahead bias
- semantica de `event time` vs `processing time`
- as-of joins o equivalentes
- timestamps de observacion, publicacion y disponibilidad
- manejo de datos fuera de orden

### 3. Calculo incremental

Verificar:

- actualizacion incremental real
- estado por ventana
- rolling/sliding windows eficientes
- streaming / near-real-time
- correccion del incremental bajo gaps y reorder

### 4. Storage y serving

Verificar:

- storage offline real o solo dumps batch
- online store real o solo cache en memoria
- acceso por entidad + timestamp
- acceso por rango
- punto de consulta para entrenamiento
- punto de consulta para inferencia/live
- persistencia exacta del valor usado en cada decision

### 5. Versionado y reproducibilidad

Verificar:

- versionado de feature definitions
- versionado de feature sets
- lineage desde datos fuente hasta feature final
- capacidad de reconstruir datasets historicos exactamente
- traza entre features usadas y modelo/backtest/inferencia

### 6. Calidad y validacion

Verificar:

- nulls / NaNs / infinites
- rangos imposibles
- features degeneradas
- desalineaciones temporales
- warm-up periods
- gaps y missing data

### 7. Online vs offline consistency

Verificar:

- reutilizacion del mismo codigo o logica
- pruebas de consistencia online/offline
- riesgo de training-serving skew
- diferencias de ordenamiento o clocks entre modos

### 8. Testing y operacion

Verificar:

- tests unitarios por feature o agregador
- tests de leakage
- tests de ventanas y warm-up
- tests de consistencia temporal
- tests de serving
- observabilidad de fallos y latencia por feature

## Formato obligatorio de salida

Usar esta estructura:

- `A. RESUMEN EJECUTIVO`
- `B. DELIMITACION FUNCIONAL`
- `C. MAPA DE CAPACIDADES DETECTADAS`
- `D. EVALUACION ESPECIFICA PARA FEATURE ENGINE + FEATURE STORE EN TRADING`
- `E. EVALUACION ARQUITECTONICA`
- `F. PREGUNTAS CRITICAS QUE UN ARQUITECTO CUANTITATIVO DEBE PODER RESPONDER`
- `G. HUECOS, RIESGOS Y DEUDA TECNICA`
- `H. PLAN DE MEJORA PRIORIZADO`
- `I. CAMBIOS CONCRETOS SUGERIDOS EN EL CODIGO`
- `J. VEREDICTO FINAL`

Cerrar siempre con:

- `TOP 10 HALLAZGOS MAS IMPORTANTES`
- `APTO / NO APTO POR NIVEL DE USO`

## Reglas de estilo

- Ser extremadamente concreto.
- Citar evidencia real: archivos, clases, funciones, tests, artifacts.
- Marcar claramente `IMPLEMENTADO`, `PARCIAL`, `AUSENTE` o `NO VERIFICABLE`.
- Explicar por que cada problema importa en terminos cuantitativos u operativos.
- No suavizar carencias estructurales.
- No dar optimismo artificial.
