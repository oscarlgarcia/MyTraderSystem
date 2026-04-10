---
name: auditoria-docs
description: Audita documentacion tecnica HTML existente revisando directamente los archivos reales para evaluar cobertura, consistencia, navegacion, profundidad, glosario y utilidad operativa. Usa esta skill cuando se necesite verificar si una documentacion de plataforma o sistema es suficiente para entender, operar, modificar, extender, probar y usar eficientemente la aplicacion.
---

# Auditoria de Documentacion HTML

Usa esta skill para auditar documentacion tecnica ya existente en HTML. La auditoria debe basarse en archivos reales, no en resúmenes previos.

## Objetivo

Determinar si la documentacion cubre de forma util:

- entendimiento de la plataforma
- operacion diaria
- modificacion y extension
- desarrollo de estrategias o funcionalidades
- pruebas y validacion
- uso eficiente por nuevos contribuidores

## Flujo de trabajo

1. Inspecciona directamente el arbol HTML indicado por el usuario.
2. Verifica estructura, paginas, headings, enlaces internos, breadcrumbs, menus e indices.
3. Revisa contenido real de paginas representativas y de detalle, no solo indices.
4. Clasifica cada area como `Completo`, `Parcial` o `Ausente`.
5. Detecta huecos entre arquitectura, dominio, modulos, flujos, datos, research, operacion, desarrollo, seguridad, decisiones y glosario.
6. Señala terminos usados pero no definidos en glosario.
7. Detecta paginas huérfanas o funcionalmente invisibles en la navegacion.
8. Distingue entre:
   - existencia estructural
   - profundidad real
   - utilidad operativa

## Criterios de clasificacion

- `Completo`: existe pagina dedicada, el contenido es especifico, coherente, enlazado y accionable.
- `Parcial`: la pagina existe pero es superficial, generica, placeholder-heavy o insuficiente para ejecutar trabajo real.
- `Ausente`: no existe pagina dedicada o solo se menciona el tema sin contenido propio.

## Señales de baja calidad

- placeholders repetidos como contenido principal
- diagramas genericos sin aterrizaje concreto
- indices correctos pero paginas hijas vacias o casi vacias
- glosario corto respecto a los terminos usados
- ausencia de enlaces cruzados entre secciones relacionadas
- inconsistencias de idioma o terminologia
- ADRs sin decisiones reales
- FAQ o runbooks sin pasos accionables

## Salida esperada

Entrega siempre:

1. Matriz de cobertura por area y subarea con `Completo / Parcial / Ausente`
2. Huecos documentales concretos
3. Inconsistencias detectadas
4. Terminos faltantes en glosario
5. Paginas que deberian crearse
6. Paginas existentes que deberian ampliarse o reestructurarse
7. Problemas de navegacion
8. Prioridad recomendada
9. Resumen ejecutivo con riesgo documental global

## Reglas

- No inventes contenido inexistente.
- Cita rutas concretas cuando señales problemas.
- Si la ruta indicada por el usuario no existe, dilo y audita la ruta real mas cercana solo si es inequívoca.
- Diferencia enlaces tecnicamente existentes de navegacion realmente usable.
