---
name: iteracion-feature
description: Skill global para ejecutar un pipeline completo de auditoria, plan de remediacion y backlog sobre un modulo de Feature Engine + Feature Store. Usala cuando se necesite revisar el modulo end-to-end y convertir el diagnostico en una secuencia ejecutable para backtesting serio, paper trading y live trading.
---

# Iteracion Feature

## Objetivo

Orquestar tres etapas encadenadas sobre un modulo de `Feature Engine + Feature Store`:

1. auditoria tecnica
2. plan de remediacion / implementacion
3. backlog tecnico ejecutable

La skill global no reemplaza las skills existentes. Las reutiliza como fuente principal y conserva sus estructuras de salida.

## Fuente principal por etapa

- Etapa 1: [auditoria-feature](C:\Users\oortega\.codex\skills\auditoria-feature\SKILL.md)
- Etapa 2: [plan-remediacion-feature](C:\Users\oortega\.codex\skills\plan-remediacion-feature\SKILL.md)
- Etapa 3: [backlog-feature](C:\Users\oortega\.codex\skills\backlog-feature\SKILL.md)

Los prompts canonicos locales viven en:

- [prompt-auditoria.md](C:\Users\oortega\.codex\skills\iteracion-feature\references\prompt-auditoria.md)
- [prompt-plan-remediacion.md](C:\Users\oortega\.codex\skills\iteracion-feature\references\prompt-plan-remediacion.md)
- [prompt-backlog.md](C:\Users\oortega\.codex\skills\iteracion-feature\references\prompt-backlog.md)

## Regla principal

Inspeccionar siempre el codigo real antes de concluir. No inventar capacidades. No saltar etapas si la entrada previa no esta claramente presente o no es verificable.

## Flujo obligatorio

### Caso por defecto

Si el usuario no aporta artefactos previos verificables:

1. abrir la skill de auditoria
2. ejecutar la auditoria usando el prompt canonico local de auditoria
3. abrir la skill de plan
4. ejecutar el plan usando como input principal la auditoria generada en la etapa anterior
5. abrir la skill de backlog
6. ejecutar el backlog usando como input principal el plan generado en la etapa anterior

### Si el usuario aporta una auditoria previa explicita

1. verificar que la auditoria existe en el contexto
2. omitir solo la etapa de auditoria
3. ejecutar plan
4. ejecutar backlog

### Si el usuario aporta un plan previo explicito

1. verificar que el plan existe en el contexto
2. omitir auditoria y plan
3. ejecutar backlog

## Reglas de salida

- No fusionar auditoria, plan y backlog en un solo bloque.
- Cada etapa debe respetar exactamente la estructura obligatoria de su skill fuente.
- Si hay divergencia entre una skill fuente y el prompt local de esta skill, prevalece el prompt local para el contenido de la etapa, manteniendo la estructura obligatoria de la skill especifica.
- La etapa de backlog debe cerrar con una pregunta explicita sobre si se desea implementar el backlog siguiendo el orden del punto `F. ORDEN EXACTO DE IMPLEMENTACION`.

## Reglas operativas

- No repetir el diagnostico al pasar de auditoria a plan.
- No repetir auditoria ni plan al pasar de plan a backlog.
- Si falta evidencia para arrancar desde plan o backlog, volver a la etapa anterior necesaria.
- Mantener el idioma por defecto en espanol.

## Uso recomendado

- "Usa $iteracion-feature para revisar end-to-end mi modulo de features."
- "Usa $iteracion-feature solo desde el plan; te paso la auditoria previa."
- "Usa $iteracion-feature solo para backlog; te paso el plan previo."
