---
name: auditoria-ingestion
description: Audita modulos de ingestion de market data para sistemas cuantitativos de trading. Usa esta skill cuando el usuario pida una revision rigurosa del pipeline de ingestion, backfill, replay, handoff historico-live, resiliencia, observabilidad, integridad temporal o readiness para research, backtesting, paper trading o live trading, y necesite un veredicto tecnico basado en codigo real.
---

# Auditoria Ingestion

## Objetivo

Emitir una auditoria tecnica dura, basada en evidencia de codigo real, para decidir si un modulo de ingestion sirve solo como prototipo o puede sostener:

- research serio
- backtesting serio y reproducible
- paper trading estable
- live trading con garantias razonables

La auditoria debe ser util para tomar decisiones de arquitectura y de roadmap. No describir el codigo por describirlo.

## Regla principal

Inspeccionar siempre el codigo real antes de concluir.

No asumir funcionalidades no demostradas.

Si algo no puede demostrarse desde:

- codigo
- configuracion
- tests
- scripts
- artifacts
- documentacion tecnica versionada

marcarlo explicitamente como `NO VERIFICABLE CON LA EVIDENCIA ACTUAL`.

## Flujo obligatorio

1. Inspeccionar el modulo completo relevante:
   - `app/ingestion`
   - `app/marketdata`
   - persistencia raw/normalized
   - replay
   - recovery / handoff
   - observabilidad
   - ops / validation / gates
   - configuracion
   - tests
   - documentacion tecnica
2. Identificar el scope real soportado hoy:
   - feeds historicos
   - feeds live
   - guarantees reales por feed
3. Separar con rigor:
   - lo implementado
   - lo parcial
   - lo ausente
   - lo no verificable
4. Evaluar el modulo como componente de una plataforma cuantitativa, no como libreria aislada.
5. Emitir veredicto por nivel de uso:
   - research
   - backtesting
   - paper
   - live

## Reglas de evaluacion

Aplicar estas reglas siempre:

- No confundir "recibo datos" con "tengo un ingestion robusto".
- No confundir "hay timestamps" con "hay semantica temporal correcta".
- No confundir "hay retry" con "hay recuperacion consistente".
- No confundir "guardo candles" con "puedo soportar backtesting reproducible".
- No confundir "hay logs" con "hay observabilidad operable".
- No confundir "funciona en demo" con "sirve para trading real".

## Criterios tecnicos obligatorios a inspeccionar

### 1. Contrato de datos

Verificar:

- modelos de eventos canonicos
- typing
- schemas raw y normalized
- metadata de instrumentos
- compatibilidad schema versioning
- errores tipados

### 2. Semantica temporal

Verificar:

- `exchange_ts`
- `provider_ts`
- `receive_ts`
- `process_ts`
- preservacion del timestamp original
- orden temporal por instrumento/stream
- manejo de out-of-order
- manejo de late events
- clock skew
- timestamps invalidos
- reconstruccion exacta del orden persistido

### 3. Integridad del feed

Verificar:

- gaps
- duplicados
- continuidad
- cursors / offsets / checkpoints
- reconciliacion tras reconexion
- catch-up
- consistencia historico vs live
- validacion de velas corruptas o inconsistentes

### 4. Persistencia y replay

Verificar:

- raw landing append-only o equivalente
- separacion raw vs normalized
- atomicidad razonable
- replay desde raw
- parity raw -> replay -> normalized
- manifests / checksums / integridad fisica
- reproducibilidad del dataset usado en backtest

### 5. Resiliencia operativa

Verificar:

- retry
- backoff
- reconnect
- heartbeat
- liveness
- rate limit handling
- circuit breaker si existe
- buffering / backpressure
- perdida silenciosa de datos
- recovery exacto o aproximado por feed

### 6. Handoff historico -> live

Verificar:

- bootstrap historico
- transicion a stream
- eliminacion de solapes
- control de huecos
- consistencia de la ventana de handoff

### 7. Observabilidad

Verificar:

- logs estructurados
- correlation / trace ids
- metricas por fuente, simbolo y stream
- gaps
- duplicates
- reconnects
- latencia
- skew temporal
- backlog
- errores por fuente
- dashboards/alerts/gates operativos reales

### 8. Testing

Verificar:

- unitarios
- integracion
- replay
- reconexion
- gaps
- duplicados
- handoff
- recovery
- contratos del vendor
- payloads corruptos
- smoke operativo

### 9. Seguridad y operacion

Verificar:

- configuracion dev/test/prod
- hardening de prod
- secretos
- filtrado accidental de secretos
- runbooks
- drill operativo
- release gates

## Sesgo de evaluacion correcto

La auditoria debe maximizar utilidad para este fin:

- llevar el modulo a backtesting serio
- llevarlo a paper trading estable
- acercarlo a live trading con garantias razonables

Por tanto:

- destacar con claridad que bloquea backtesting
- destacar con claridad que bloquea paper
- destacar con claridad que bloquea live
- no gastar espacio en observaciones cosmeticas
- priorizar riesgos de integridad temporal, replay, resiliencia y operacion

## Formato obligatorio de salida

Usar esta estructura salvo que el usuario exija otra aun mas estricta:

- `A. RESUMEN EJECUTIVO`
- `B. DELIMITACION FUNCIONAL DEL MODULO`
- `C. MAPA DE CAPACIDADES DETECTADAS`
- `D. EVALUACION ESPECIFICA PARA MARKET DATA`
- `E. EVALUACION ARQUITECTONICA`
- `F. PREGUNTAS CRITICAS DE ARQUITECTURA CUANTITATIVA`
- `G. HUECOS, RIESGOS Y DEUDA TECNICA`
- `H. PLAN DE MEJORA PRIORIZADO`
- `I. CAMBIOS CONCRETOS SUGERIDOS EN EL CODIGO`
- `J. VEREDICTO FINAL`

Cerrar siempre con:

- `TOP 10 HALLAZGOS MAS IMPORTANTES`
- `APTO / NO APTO POR NIVEL DE USO`

## Contenido minimo por seccion

### A. RESUMEN EJECUTIVO

Incluir:

- nivel de madurez:
  - prototipo / research / paper / pre-live / produccion robusta
- fortalezas principales
- debilidades principales
- riesgos criticos
- veredicto preliminar

### B. DELIMITACION FUNCIONAL

Responder explicitamente:

- que responsabilidad exacta tiene ingestion
- donde empieza y donde debe terminar
- si hay mezcla indebida con trading / feature engineering / señales
- si produce raw, normalized o ambos
- si el contrato con consumidores esta claro o no

### C. MAPA DE CAPACIDADES

Para cada capacidad:

- capacidad
- estado:
  - `IMPLEMENTADO`
  - `PARCIAL`
  - `AUSENTE`
  - `NO VERIFICABLE`
- evidencia
- comentario tecnico

### D. EVALUACION ESPECIFICA PARA MARKET DATA

Cubrir explicitamente:

1. fuentes y tipos de datos
2. modos de ingestion
3. tiempo y orden temporal
4. integridad del feed
5. idempotencia y semantica de entrega
6. normalizacion
7. calidad de datos
8. resiliencia operativa
9. persistencia y almacenamiento
10. multi-asset / multi-venue / multi-timeframe
11. rendimiento y latencia
12. observabilidad
13. testing
14. seguridad y operacion

### E. EVALUACION ARQUITECTONICA

Evaluar con criterio de arquitecto:

- separacion de responsabilidades
- modularidad
- extensibilidad
- testabilidad
- acoplamiento
- cohesion
- claridad de interfaces
- capacidad para nuevas fuentes
- capacidad para nuevos tipos de datos
- future scale

### F. PREGUNTAS CRITICAS DE ARQUITECTURA CUANTITATIVA

Responder explicitamente:

- que invariantes intenta garantizar el modulo
- contrato exacto hacia feature store / backtester / señales
- semantica temporal real
- semantica de entrega real
- comportamiento ante corte de red
- comportamiento ante duplicados y stale data
- comportamiento ante schema drift del vendor
- parte mas fragil
- parte que peor escala
- parte que mas arriesga research/backtest
- parte que impediria migrar a live con garantias

### G. HUECOS, RIESGOS Y DEUDA

Separar:

1. huecos funcionales
2. riesgos de integridad de datos
3. riesgos temporales
4. riesgos de escalabilidad
5. riesgos operativos
6. riesgos para backtesting reproducible
7. deuda tecnica
8. decisiones peligrosas o implicitas

### H. PLAN DE MEJORA PRIORIZADO

Ordenar en:

- `PRIORIDAD CRITICA`
- `PRIORIDAD ALTA`
- `PRIORIDAD MEDIA`
- `MEJORAS DESEABLES`

Para cada item incluir:

- problema
- impacto
- riesgo mitigado
- complejidad aproximada
- recomendacion concreta

### I. CAMBIOS CONCRETOS EN CODIGO

Proponer:

- refactors concretos
- abstracciones faltantes
- interfaces
- validadores
- checkpoint / cursor persistence
- gaps
- dedup
- observabilidad
- estructura de tests
- estrategia raw/normalized
- estrategia historico + live handoff
- estrategia de replay

### J. VEREDICTO FINAL

Decir explicitamente si lo apruebas para:

- research
- backtesting serio
- paper trading
- live trading

Y bajo que condiciones.

## Reglas de estilo

- Ser extremadamente concreto.
- No meter teoria innecesaria.
- No sonar optimista si la evidencia no lo soporta.
- No suavizar carencias estructurales.
- Explicar por que cada problema importa en terminos cuantitativos u operativos.

## Cierre obligatorio

### TOP 10 HALLAZGOS MAS IMPORTANTES

Exactamente 10 puntos ordenados por criticidad.

Para cada punto incluir:

- hallazgo
- por que importa
- riesgo real
- accion recomendada

### APTO / NO APTO POR NIVEL DE USO

Incluir mini tabla con:

- research
- backtesting
- paper trading
- live trading

Marcando:

- `SI`
- `CON RESERVAS`
- `NO`

## Referencias de evidencia

Al citar archivos del workspace, usar rutas completas y absolutas.
