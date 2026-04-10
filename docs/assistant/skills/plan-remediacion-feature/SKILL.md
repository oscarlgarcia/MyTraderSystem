---
name: plan-remediacion-feature
description: Convierte una auditoria tecnica previa de un modulo de Feature Engine + Feature Store en un plan de remediacion ejecutable, priorizado y verificable para llevarlo a backtesting serio, paper trading y live trading con garantias temporales, reproducibilidad, versionado, online/offline consistency y serving trazable.
---

# Plan Remediacion Feature

## Objetivo

Tomar como entrada principal una auditoria tecnica previa del modulo de Feature Engine + Feature Store y transformarla en un plan de remediacion ejecutable.

No repetir la auditoria salvo para justificar acciones concretas.

El foco es cerrar huecos reales en:

- point-in-time correctness
- ausencia de leakage
- versionado de definiciones y outputs
- reproducibilidad
- online/offline consistency
- serving trazable
- observabilidad y operacion

## Regla principal

Priorizar garantias reales, no abstracciones cosmeticas.

Si una parte no puede elevarse a paper o live sin redisenarse, decirlo claramente.

Si una mejora depende de otra, explicitar la dependencia.

## Flujo obligatorio

1. Partir de la auditoria previa como fuente principal.
2. Contrastar con el codigo actual solo si hace falta validar que el plan encaja con la arquitectura existente.
3. Traducir hallazgos a:
   - brechas
   - prioridades
   - cambios tecnicos concretos
   - criterios de aceptacion verificables
   - fases de implementacion
4. Separar con rigor:
   - minimo para backtesting serio
   - minimo adicional para paper trading
   - minimo adicional para live trading
5. Diferenciar:
   - quick wins
   - refactors estructurales
   - redisenos necesarios
   - validaciones
   - testing
   - observabilidad
   - operacion
6. Cerrar con checklist go/no-go por nivel.

## Criterios obligatorios del plan

El plan debe cubrir explicitamente:

- definicion formal de features
- semantica temporal
- control de leakage
- joins point-in-time / as-of
- DAG de dependencias
- calculo incremental correcto
- offline store y online serving
- versionado reproducible
- lineage y trazabilidad
- validacion de outputs
- parity online/offline
- tests temporales y de reproducibilidad
- recovery y operabilidad para paper/live

## Formato obligatorio de salida

Usar esta estructura:

- `A. RESUMEN EJECUTIVO DEL PLAN`
- `B. MAPA DE BRECHAS A CERRAR`
- `C. CRITERIOS DE APROBACION POR NIVEL DE MADUREZ`
- `D. PLAN DE MEJORAS PRIORIZADO`
- `E. ROADMAP POR FASES`
- `F. CAMBIOS DE ARQUITECTURA EXPLICITOS`
- `G. CAMBIOS EXPLICITOS EN CODIGO Y ESTRUCTURA DEL REPOSITORIO`
- `H. PLAN ESPECIFICO DE HARDENING PARA FEATURE ENGINE + FEATURE STORE`
- `I. PLAN ESPECIFICO DE TESTING Y VALIDACION`
- `J. CRITERIOS DE ACEPTACION DETALLADOS`
- `K. PLAN DE EJECUCION OPERATIVA`
- `L. RIESGOS RESIDUALES Y TRADE-OFFS`
- `M. TOP 15 ACCIONES MAS IMPORTANTES`
- `N. VEREDICTO FINAL Y CHECKLIST DE SALIDA`

## Reglas de estilo

- Ser muy concreto y ejecutable.
- No dar recomendaciones genericas.
- No decir "mejorar versionado" sin explicar como.
- No decir "evitar leakage" sin indicar donde se garantiza.
- No decir "anadir tests" sin decir cuales, en que orden y con que objetivo.
- Usar prioridades, dependencias y criterios de aceptacion verificables.
- Separar claramente lo necesario para backtesting, paper y live.
