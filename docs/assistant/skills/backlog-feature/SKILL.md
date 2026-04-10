---
name: backlog-feature
description: Convierte una auditoria tecnica y un plan de remediacion previos de un modulo de Feature Engine + Feature Store en un backlog tecnico ejecutable, priorizado y verificable, con epicas, historias, tareas, dependencias, criterios de aceptacion y secuencia de implementacion para llegar a backtesting serio, paper trading y live trading.
---

# Backlog Feature

## Objetivo

Transformar una auditoria previa y un plan de remediacion de un modulo de Feature Engine + Feature Store en backlog de ingenieria real.

No repetir la auditoria ni el plan conceptual salvo para justificar items concretos.

## Regla principal

Priorizar items que aumenten garantias reales en:

- point-in-time correctness
- ausencia de leakage
- reproducibilidad
- versionado
- online/offline consistency
- serving trazable
- operabilidad

Excluir tareas cosmeticas o backlog inflado.

## Flujo obligatorio

1. Partir de la auditoria y el plan previos como fuente principal.
2. Contrastar con el codigo actual solo si hace falta afinar dependencias o alcance.
3. Agrupar el trabajo en epicas coherentes.
4. Desglosar en historias, tareas, hardening, refactors y tests.
5. Indicar para cada item:
   - prioridad
   - impacto por nivel de madurez
   - dependencias
   - criterios de aceptacion verificables
   - orden recomendado
6. Separar explicitamente:
   - minimo para backtesting serio
   - minimo adicional para paper trading
   - minimo adicional para live trading
7. Cerrar con secuencia exacta de implementacion y checklist go/no-go.

## Areas obligatorias del backlog

Cubrir explicitamente, si aplica:

- definicion y registro de features
- semantica temporal y causalidad
- arquitectura Feature Engine / Feature Store
- DAG de dependencias
- calculo incremental
- versionado y reproducibilidad
- offline/online consistency
- serving
- calidad de features
- observabilidad
- testing
- seguridad y operacion
- escalabilidad y performance

## Formato obligatorio de salida

Usar esta estructura:

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

## Reglas de estilo

- Ser muy concreto y ejecutable.
- No fragmentar en exceso tareas triviales.
- No mezclar epicas, historias y tareas sin estructura.
- No decir "anadir versionado" sin especificar que versionar y como.
- No decir "anadir parity online/offline" sin explicar como comprobarla.
- No decir "evitar leakage" sin traducirlo a cambios concretos de arquitectura, validacion y tests.
- Priorizar el orden real de implementacion, no solo la severidad abstracta.
