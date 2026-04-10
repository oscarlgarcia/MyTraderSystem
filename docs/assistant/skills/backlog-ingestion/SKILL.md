---
name: backlog-ingestion
description: Convierte una auditoria tecnica previa y un plan de remediacion de un modulo de ingestion de market data en un backlog tecnico ejecutable. Usa esta skill cuando el usuario no quiera otro informe ni otro plan conceptual, sino epicas, historias, tareas, dependencias, prioridades, criterios de aceptacion y secuencia de implementacion para llegar a backtesting serio, paper trading y live trading.
---

# Backlog Ingestion

## Objetivo

Transformar una auditoria previa y un plan de mejoras previo en un backlog tecnico ejecutable, estructurado y listo para implementacion por un equipo de ingenieria.

La salida debe servir para ejecutar trabajo real, no para seguir diagnosticando.

## Regla principal

Tomar como fuente principal:

- auditoria previa
- plan de remediacion previo
- estado actual del codigo cuando haga falta refinar dependencias o descartar trabajo ya cubierto

No volver a auditar salvo para validar si una tarea sigue siendo necesaria.

## Flujo obligatorio

1. Partir del estado actual real del modulo.
2. Eliminar del backlog cualquier tarea ya implementada o puramente cosmetica.
3. Traducir brechas y riesgos residuales a:
   - epicas
   - historias tecnicas
   - hardening
   - tests
   - tareas operativas
4. Marcar para cada item:
   - prioridad
   - impacto
   - dependencias
   - esfuerzo
   - criterio de aceptacion verificable
5. Separar con precision:
   - minimo para backtesting serio
   - minimo adicional para paper
   - minimo adicional para live

## Reglas de diseno del backlog

- No crear backlog inflado artificialmente.
- No fragmentar tareas triviales.
- No repetir el informe previo.
- No usar lenguaje vago.
- No decir "mejorar observabilidad" sin listar metricas, alertas y artifacts concretos.
- No decir "anadir replay" sin decir desde donde, como y con que garantias.
- No decir "refactorizar" sin concretar componente, objetivo y orden.
- Si una mejora exige rediseño, marcarla como tal.
- Si un item solo sirve para live, decirlo claramente.

## Sesgo correcto de priorizacion

Priorizar siempre en este orden:

1. integridad de datos
2. semantica temporal
3. raw / replay / parity / reproducibilidad
4. gaps / duplicates / recovery
5. observabilidad operativa real
6. hardening de configuracion y promocion
7. soak / vendor contracts / live drill
8. robustecimiento posterior

No priorizar:

- cosmetica
- reorganizacion sin impacto real
- nuevos feeds si no aumentan readiness inmediato

## Estructura obligatoria de salida

Usar estas secciones salvo que el usuario pida otra aun mas estricta:

- `A. RESUMEN DEL BACKLOG`
- `B. EPICAS PRINCIPALES`
- `C. BACKLOG DETALLADO`
- `D. DESGLOSE OBLIGATORIO POR AREAS`
- `E. BACKLOG POR NIVEL DE MADUREZ`
- `F. ORDEN EXACTO DE IMPLEMENTACION`
- `G. QUICK WINS VS REFACTORS ESTRUCTURALES`
- `H. BACKLOG EN FORMATO TIPO ISSUE TRACKER`
- `I. CRITERIOS DE ACEPTACION GLOBALES`
- `J. RIESGOS SI SE RECORTA EL BACKLOG`
- `K. TOP 20 ITEMS MAS IMPORTANTES`
- `L. VEREDICTO FINAL`

## Contenido minimo por seccion

### A. RESUMEN DEL BACKLOG

Incluir:

- objetivo general
- estado actual resumido
- estrategia de implementacion
- bloques principales
- riesgos si no se ejecuta

### B. EPICAS PRINCIPALES

Para cada epica incluir:

- ID de epica
- nombre
- objetivo
- problema que resuelve
- impacto en backtesting / paper / live
- prioridad global
- dependencias de alto nivel

### C. BACKLOG DETALLADO

Usar items con formato tipo:

- ID: `ING-XXX`
- Epica
- Titulo
- Tipo
- Prioridad
- Impacto objetivo
- Problema actual
- Riesgo que mitiga
- Descripcion tecnica explicita
- Cambios concretos en codigo/arquitectura
- Dependencias
- Criterios de aceptacion verificables
- Evidencia en auditoria previa
- Esfuerzo estimado
- Orden recomendado
- Notas de implementacion

Los items deben ser lo bastante detallados como para que un ingeniero no tenga que reinterpretar demasiado.

### D. DESGLOSE OBLIGATORIO POR AREAS

Cubrir explicitamente:

1. contratos y modelos de datos
2. time semantics
3. persistencia y almacenamiento
4. replay y reproducibilidad
5. estado y recuperacion
6. integridad de feed
7. historico -> live handoff
8. resiliencia de conectores
9. observabilidad
10. testing
11. seguridad y operacion
12. escalabilidad

### E. BACKLOG POR NIVEL DE MADUREZ

Separar:

1. minimo para backtesting serio
2. minimo adicional para paper
3. minimo adicional para live

Para cada nivel:

- obligatorios
- recomendables
- diferibles

### F. ORDEN EXACTO DE IMPLEMENTACION

Dar una secuencia paso a paso.

Para cada paso incluir:

- que desbloquea
- por que va antes
- que riesgo reduce
- que dependencias satisface

### G. QUICK WINS VS REFACTORS ESTRUCTURALES

Separar:

- quick wins de alto impacto
- mejoras estructurales incrementales
- refactors profundos o redisenos
- trabajo obligatorio previo a live

### H. ISSUE TRACKER

Incluir tabla compacta con:

- ID
- titulo
- prioridad
- impacto
- esfuerzo
- dependencias
- estado objetivo

### I. CRITERIOS DE ACEPTACION GLOBALES

Definir condiciones verificables para decir:

- apto para backtesting serio
- apto para paper trading
- apto para live trading

### J. RIESGOS SI SE RECORTA EL BACKLOG

Explicar que ocurre si se intenta aprobar:

- backtesting
- paper
- live

sin completar ciertos bloques.

### K. TOP 20 ITEMS MAS IMPORTANTES

Exactamente 20 items ordenados por prioridad real.

Para cada uno incluir:

- ID
- titulo
- por que esta tan arriba
- que riesgo evita
- que nivel desbloquea

### L. VEREDICTO FINAL

Cerrar con:

- si el backlog es incremental o exige rehacer partes
- que bloque es mas critico
- que secuencia minima exigir antes de acercarse a live

## Reglas de numeracion

- Mantener continuidad con la numeracion ya existente del proyecto.
- Si ya existe una serie pendiente en `ING-200+`, continuar en esa serie.
- No introducir saltos arbitrarios de numeracion.

## Reglas de estilo

- Muy estructurado
- Muy concreto
- Ejecutable
- Sin relleno
- Sin teoria innecesaria
- Con foco en implementacion real

## Referencias de evidencia

Si se citan archivos del workspace, usar rutas completas y absolutas.
