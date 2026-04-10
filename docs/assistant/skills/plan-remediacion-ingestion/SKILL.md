---
name: plan-remediacion-ingestion
description: Convierte una auditoria tecnica previa de un modulo de ingestion de market data en un plan de remediacion ejecutable. Usa esta skill cuando el usuario ya tenga un diagnostico o auditoria y necesite una hoja de ruta tecnica, priorizada y verificable para llevar el modulo a backtesting serio, paper trading estable y live trading con garantias razonables.
---

# Plan Remediacion Ingestion

## Objetivo

Transformar una auditoria previa de ingestion en un plan tecnico ejecutable, orientado a:

- backtesting serio y reproducible
- paper trading estable
- live trading con garantias razonables

No volver a auditar salvo que sea imprescindible para validar si una mejora encaja con la arquitectura actual.

## Regla principal

Tomar como fuente principal:

- la auditoria previa
- backlog o planes previos si existen
- el estado actual del codigo cuando haga falta contrastar una recomendacion

La salida debe ser un plan de remediacion, no otro informe abstracto.

## Flujo obligatorio

1. Partir del ultimo diagnostico fiable disponible.
2. Identificar solo las brechas que siguen importando para:
   - integridad temporal
   - reproducibilidad
   - replay
   - resiliencia
   - observabilidad operativa
   - readiness por nivel
3. Traducir esas brechas a decisiones, cambios y fases de implementacion.
4. Diferenciar siempre:
   - minimo para backtesting serio
   - minimo adicional para paper
   - minimo adicional para live
5. Indicar dependencias, orden recomendado y criterios de aceptacion verificables.

## Reglas de evaluacion y diseno del plan

- No repetir el diagnostico salvo para justificar una accion.
- No dar recomendaciones vagas.
- No decir "refactorizar arquitectura" sin decir que componente cambia.
- No decir "mejorar observabilidad" sin decir:
  - metricas
  - alertas
  - dashboards
  - artifacts
- No decir "anadir tests" sin decir:
  - que casos
  - que riesgo cubren
  - que criterio de aceptacion desbloquean
- Priorizar cambios que aumenten garantias reales, no cosmetica.
- Si algo exige rediseño, decirlo sin suavizarlo.

## Sesgo correcto del plan

El plan debe servir para mover el sistema hacia un componente confiable de plataforma cuantitativa.

Por tanto, priorizar:

1. integridad de datos
2. semantica temporal
3. raw/replay/parity
4. gaps / duplicates / recovery
5. metadata de instrumentos y gobierno del runtime
6. observabilidad operativa real
7. hardening de configuracion y promocion
8. evidencia operativa y testing

No priorizar:

- limpieza cosmética
- reorganizaciones sin impacto real
- nuevas features que no aumenten readiness

## Formato obligatorio de salida

Usar esta estructura salvo que el usuario pida otra aun mas estricta:

- `A. RESUMEN EJECUTIVO DEL PLAN`
- `B. MAPA DE BRECHAS A CERRAR`
- `C. CRITERIOS DE APROBACION POR NIVEL DE MADUREZ`
- `D. PLAN DE MEJORAS PRIORIZADO`
- `E. ROADMAP POR FASES`
- `F. CAMBIOS DE ARQUITECTURA EXPLICITOS`
- `G. CAMBIOS EXPLICITOS EN CODIGO Y ESTRUCTURA DEL REPOSITORIO`
- `H. PLAN ESPECIFICO DE HARDENING PARA MARKET DATA INGESTION`
- `I. PLAN ESPECIFICO DE TESTING Y VALIDACION`
- `J. CRITERIOS DE ACEPTACION DETALLADOS`
- `K. PLAN DE EJECUCION OPERATIVA`
- `L. RIESGOS RESIDUALES Y TRADE-OFFS`
- `M. TOP 15 ACCIONES MAS IMPORTANTES`
- `N. VEREDICTO FINAL Y CHECKLIST DE SALIDA`

## Contenido minimo por seccion

### A. RESUMEN EJECUTIVO DEL PLAN

Incluir:

- estado actual resumido
- objetivo de madurez
- estrategia general
- bloques principales de trabajo
- riesgos si no se ejecuta

### B. MAPA DE BRECHAS

Para cada brecha incluir:

- brecha detectada
- severidad
- impacto en backtesting
- impacto en paper
- impacto en live
- causa raiz
- accion correctiva
- prioridad

### C. CRITERIOS DE APROBACION

Definir condiciones minimas para:

- backtesting serio
- paper trading
- live trading

Para cada nivel incluir:

- requisitos obligatorios
- requisitos recomendables
- anti-patrones bloqueantes

### D. PLAN DE MEJORAS PRIORIZADO

Ordenar en:

- `PRIORIDAD CRITICA`
- `PRIORIDAD ALTA`
- `PRIORIDAD MEDIA`
- `PRIORIDAD DESEABLE`

Para cada mejora incluir:

- nombre
- problema exacto
- por que importa
- impacto por nivel
- riesgo mitigado
- complejidad
- dependencias
- recomendacion tecnica concreta
- criterio de aceptacion verificable

### E. ROADMAP POR FASES

Construir fases realistas, por ejemplo:

- estabilizacion minima
- apto para backtesting
- apto para paper
- apto para live
- robustecimiento posterior

Para cada fase incluir:

- objetivo
- alcance
- cambios
- por que van ahi
- dependencias
- entregables
- riesgos si se omite
- definition of done
- criterios de aceptacion
- tests necesarios

### F. CAMBIOS DE ARQUITECTURA EXPLICITOS

Evaluar explicitamente si hay que introducir o reforzar:

- raw landing
- normalized
- validacion
- dedup
- checkpoints/cursors
- gap detector
- replay
- handoff historico-live
- source abstractions
- pipeline desacoplado
- buffers/backpressure
- catalogo de instrumentos
- control de timestamps
- observabilidad

Para cada cambio decir:

- por que hace falta
- que problema resuelve
- si es imprescindible o recomendable
- si es incremental o exige refactor
- impacto esperado

### G. CAMBIOS EN CODIGO Y REPO

Proponer:

- modulos a separar
- clases e interfaces
- responsabilidades a mover
- validadores
- persistencia nueva
- state management
- contratos y esquemas
- errores tipados
- trazas y metricas
- estructura de tests

Siempre indicar:

- que
- como
- objetivo
- orden

### H. HARDENING DE MARKET DATA INGESTION

Cubrir explicitamente:

1. time semantics
2. integridad de feed
3. persistencia y replay
4. resiliencia
5. multi-asset y escalabilidad
6. observabilidad

### I. TESTING Y VALIDACION

Dividir entre:

1. unitarios
2. integracion
3. temporales
4. resiliencia
5. replay
6. duplicados
7. gaps
8. handoff historico-live
9. rendimiento/carga
10. smoke/operativos

Para cada categoria incluir:

- que probar
- por que
- ejemplos
- riesgo que detecta
- minimo aceptable

### J. CRITERIOS DE ACEPTACION DETALLADOS

Definir criterios verificables para cada mejora critica y alta.

No usar frases vagas.

### K. PLAN DE EJECUCION OPERATIVA

Incluir:

- orden recomendado
- quick wins reales
- que no tocar demasiado pronto
- que parchear
- que redisenar bien desde el principio
- como migrar sin romper
- feature flags
- shadow mode
- doble escritura
- comparacion old vs new

### L. RIESGOS RESIDUALES Y TRADE-OFFS

Separar:

- riesgos asumibles para backtesting
- riesgos asumibles para paper
- riesgos no asumibles para live

Y explicar trade-offs:

- complejidad vs robustez
- latencia vs validacion
- simplicidad vs replay
- desacoplamiento vs esfuerzo

### M. TOP 15 ACCIONES MAS IMPORTANTES

Exactamente 15 acciones, ordenadas por prioridad real.

Para cada una incluir:

- accion
- impacto
- esfuerzo
- urgencia
- nivel que desbloquea

### N. VEREDICTO FINAL Y CHECKLIST DE SALIDA

Cerrar con:

1. estado actual por nivel:
   - APTO / NO APTO
2. cambios minimos para aprobar:
   - backtesting
   - paper
   - live
3. checklist go/no-go:
   - terminado
   - validado
   - testeado
   - monitorizado

## Reglas de estilo

- Ser muy concreto.
- Orientar siempre a ejecucion.
- Ser critico y pragmatico.
- No repetir teoria.
- No inflar backlog ni roadmap artificialmente.
- No priorizar cambios cosmeticos.

## Referencias de evidencia

Si se citan archivos del workspace, usar rutas completas y absolutas.
