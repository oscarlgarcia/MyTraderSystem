---
name: actualizacion-general-docs
description: Actualiza documentacion tecnica HTML de forma incremental, conservando contenido valido y pidiendo siempre los parametros contexto y objetivo si faltan. Usa esta skill cuando se necesite modificar una documentacion HTML ya existente sin sobrescribirla ciegamente, manteniendo trazabilidad, consistencia y utilidad operativa.
---

# Actualizacion General de Documentacion HTML

Usa esta skill para actualizar documentacion HTML existente de forma incremental y segura. La actualizacion debe basarse en los archivos reales, no en resúmenes previos ni en suposiciones sobre la plataforma.

## Proposito

Mantener y ampliar una documentacion completa de una plataforma de trading algoritmico para que una persona sin conocimientos previos de la aplicacion ni de trading pueda:

- entender la plataforma
- comprender sus modulos y flujos
- operarla
- modificarla
- extenderla
- generar nuevas estrategias
- probarlas
- utilizar la aplicacion con eficiencia

## Entradas obligatorias

Esta skill requiere siempre dos parametros logicos:

- `contexto`
- `objetivo`

Si falta uno:

- pide solo el parametro faltante antes de continuar

Si faltan ambos:

- pide ambos explicitamente y no continues con la actualizacion

## Regla operativa sobre la ruta

Cuando el usuario pida revisar `/docs/docs-html`, comprueba primero si esa ruta existe.

- Si existe, usa esa ruta.
- Si no existe, solo usa la ruta real mas cercana si la equivalencia es inequivoca.
- En este workspace existe una discrepancia ya conocida entre la ruta canonica de publicacion `docs/docs-html` y la ruta fisica actual `docs-html`.
- Si esa discrepancia afecta la respuesta, debes dejarla reflejada en `Informacion pendiente` o `Riesgos o inconsistencias detectadas`.

## Reglas de actualizacion incremental

- No sobrescribas paginas completas si no es necesario.
- Conserva secciones validas ya existentes.
- Anade subsecciones nuevas donde falten.
- Reemplaza solo bloques concretos que esten obsoletos, incompletos o en conflicto.
- Marca explicitamente como `[OBSOLETO]` o `Deprecado` el contenido anterior cuando corresponda, salvo que se pida limpieza explicita.
- Mantén la estructura HTML existente si es razonable.
- Mantén consistencia terminologica con el glosario.
- No inventes datos concretos de la aplicacion si no han sido proporcionados.
- Si falta informacion, añade una subseccion `Informacion pendiente` o `Huecos conocidos`.
- Si una nueva pieza de contexto impacta varias secciones, actualiza todas las secciones afectadas, no solo una.
- Todos los diagramas deben estar en Mermaid.
- Cuando el objetivo incluya una exportacion visual del diagrama, prioriza SVG sobre PNG salvo que el usuario pida expresamente raster.
- Cuando una pagina publique una exportacion visual del Mermaid, no dupliques el diagrama mostrando a la vez la exportacion y un segundo render visible del Mermaid salvo que el usuario lo pida expresamente.
- Cuando una actualizacion incluya un diagrama Mermaid nuevo o modificado, incluye tambien el bloque Mermaid generado en la respuesta final, no solo el HTML resultante.
- La documentacion debe seguir en espanol.
- No elimines cobertura documental existente salvo instruccion expresa.

## Regla de indexado de busqueda

- Tras cualquier cambio documental que afecte HTML, navegacion, paginas indexables o artefactos visibles del portal, debes ejecutar siempre el wrapper de indexado de busqueda.
- El wrapper canonico es `python scripts/docs_search_sync.py --backend static --mode incremental --docs-root docs-html`.
- No llames directamente a `scripts/generate_docs_html.py` como paso final de indexado salvo que estes manteniendo o reparando el propio wrapper.
- El wrapper es el punto de entrada estable para futuras migraciones a `db` o `engine`.
- Si el wrapper falla:
  - no des la actualizacion documental por cerrada
  - refleja el fallo en `Informacion pendiente` o `Riesgos o inconsistencias detectadas`
  - incluye el comando exacto para relanzar el indexado
  - indica, si se puede inferir, la causa probable y la forma de solventarla
- Si la actualizacion no ha cambiado nada material, puedes indicar que el indexado ha quedado `up_to_date`, pero la skill debe seguir intentando la sincronizacion.

## Secciones documentales a considerar siempre

Debes tener presentes todas estas areas, aunque no todas se modifiquen en cada ejecucion:

- Home / portal principal
- Arquitectura
  - contexto del sistema
  - capability map
  - arquitectura alto nivel
  - deployment
  - runtime topology
  - dependencias
  - observabilidad
  - health model
  - resiliencia/failover
  - NFRs
  - ADRs
- Dominio trading
  - modelo de dominio
  - ordenes
  - fills
  - posiciones y balances
  - riesgo
  - execution semantics
  - exchanges
- Modulos
  - ingestion
  - exchange adapter
  - OMS
  - execution
  - portfolio
  - risk engine
  - research
  - backtesting/simulation
  - feature store
  - control plane
- Flujos y secuencias
  - market data flow
  - order lifecycle
  - reconciliation
  - risk decision flow
  - strategy to execution
  - startup/recovery
  - recovery de market data
  - incident/recovery
- Datos
  - data catalog
  - event schemas/contracts
  - data quality
  - instrument master
  - canonical data model
  - data flow
  - lineage
- Research / Backtesting / Simulation
  - workflow de research
  - assumptions del backtester
  - simulation fidelity
  - promotion path
  - reproducibilidad
  - lineage a produccion
- Operacion
  - service catalog
  - runbooks
  - playbooks
  - SLO/SLA/alerting
  - DR/BCP
  - deployment/release
  - troubleshooting
- Desarrollo
  - onboarding
  - coding standards
  - interface/API standards
  - testing strategy
  - environment/config
- Seguridad y gobernanza
  - security architecture
  - secrets/credentials
  - auditability/controls
  - access model
- Decisiones / ADRs
- Glosario
  - trading
  - aplicacion
  - desarrollo
  - performance/operacion
- FAQ / postmortems

## Flujo de trabajo

1. Revisa directamente los archivos HTML reales del arbol indicado.
2. Determina que paginas deben actualizarse.
3. Determina si hace falta crear paginas nuevas para cubrir huecos reales.
4. Propón cambios minimos y seguros antes de tocar contenido.
5. Actualiza el HTML de forma incremental, conservando lo valido.
6. Actualiza indices y navegacion si aparecen nuevas paginas o enlaces.
7. Ejecuta el wrapper de indexado de busqueda al final de la actualizacion.
8. Valida si el wrapper devolvio exito y registra el resultado en la salida final.
9. Si cambian terminos o conceptos, actualiza tambien el glosario.
10. Si cambian decisiones arquitectonicas, sugiere crear o actualizar ADRs.
11. Si cambian flujos, actualiza tambien los diagramas Mermaid relacionados.
12. Si se genera o modifica Mermaid, refleja tambien el bloque Mermaid en la seccion `Contenido actualizado`.
13. Si se genera una exportacion visual del Mermaid, indicar la ruta del SVG generado y, si aplica, el bloque HTML que lo consume.
14. Si la pagina renderiza una exportacion SVG o PNG, dejar una unica visualizacion principal clicable en HTML salvo instruccion explicita de mostrar tambien el Mermaid visible.
15. Si algo no esta claro, dilo y deja el hueco; no inventes.

## Regla de no invencion y huecos conocidos

- No alucines detalles tecnicos de la plataforma.
- No conviertas placeholders en hechos.
- Si no hay informacion suficiente para completar un bloque, dejalo como `Informacion pendiente` o `Huecos conocidos`.
- Si una pagina existe pero el cambio pedido no la afecta, no la reescribas.
- Si el contenido actual es valido, mantenlo.

## Formato de salida obligatorio

Devuelve siempre la respuesta con esta estructura exacta:

### A. Resumen de impacto documental

- secciones afectadas
- paginas a crear
- paginas a actualizar
- paginas potencialmente obsoletas

### B. Plan de actualizacion incremental

Para cada archivo:

- ruta
- accion: crear / actualizar / ampliar / marcar obsoleto
- motivo

### C. Contenido actualizado

Para cada archivo afectado:

- ruta
- contenido HTML completo si es nuevo
- o bloque(s) HTML concretos a reemplazar / insertar si ya existe
- si aplica, indicar exactamente donde insertar el bloque
- si el cambio incluye Mermaid, incluir tambien el bloque Mermaid generado junto al bloque HTML correspondiente
- si el cambio incluye exportacion visual del Mermaid, indicar tambien la ruta del SVG generado o actualizado

### D. Cambios incorporados

Lista clara y concreta.

Incluye tambien un bloque breve de estado de indexado:

- backend de indexado
- artefactos actualizados
- resultado
- comando de reintento si falla

### E. Informacion pendiente

Lista clara y concreta.

### F. Riesgos o inconsistencias detectadas

Solo si aplica.

## Recordatorios finales

- Prioriza consistencia, trazabilidad y utilidad practica.
- No hagas un rediseño arbitrario.
- No reinicies la documentacion.
- No borres contenido valido sin dejar rastro cuando deba conservarse contexto historico.
- Esta skill es de actualizacion incremental, no de reescritura total.
