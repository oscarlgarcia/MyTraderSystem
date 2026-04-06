from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path
import re
import subprocess
import shutil
import tempfile


TODAY = date.today().isoformat()
ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs-html"
HOME_DIAGRAM_SOURCE = Path(r"C:\Users\oortega\OneDrive - BOARD\Desktop\arquitectura.png")
HOME_DIAGRAM_NAME = "arquitectura-home.png"
CANONICAL_PUBLICATION_ROUTE = "/docs/docs-html/"
CURRENT_WORKSPACE_ROUTE = "/docs-html/"
MERMAID_DIAGRAMS_DIRNAME = "diagrams"
MERMAID_VENDOR_SOURCE = ROOT / "scripts" / "vendor" / "mermaid.min.js"
BROWSER_CANDIDATES = [
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
]


@dataclass
class Page:
    section: str
    slug: str
    title: str
    summary: str
    kind: str

    @property
    def filename(self) -> str:
        return "index.html" if self.slug == "index" else f"{self.slug}.html"

    @property
    def path(self) -> Path:
        return DOCS / self.section / self.filename if self.section else DOCS / self.filename

    @property
    def url(self) -> str:
        return self.filename if not self.section else f"{self.section}/{self.filename}"

    @property
    def doc_id(self) -> str:
        return f"{self.section or 'portal'}/{self.slug}"


SECTIONS = {
    "": {
        "name": "Inicio",
        "pages": [
            ("index", "Portal Tecnico de Trading Algoritmico", "Punto de entrada principal de la documentacion.", "home"),
            ("coverage-index", "Matriz de Cobertura Documental", "Indice global de estado y placeholders.", "coverage"),
        ],
    },
    "arquitectura": {
        "name": "Arquitectura",
        "pages": [
            ("index", "Indice de Arquitectura", "Vista estructural de la plataforma.", "section-index"),
            ("contexto-sistema", "Contexto del Sistema", "Limites y actores del sistema.", "architecture"),
            ("capability-map", "Mapa de Capacidades", "Mapa de capacidades funcionales y tecnicas.", "architecture"),
            ("arquitectura-alto-nivel", "Arquitectura de Alto Nivel", "Vista de bloques principales.", "architecture"),
            ("deployment", "Despliegue", "Unidades desplegables y entornos.", "architecture"),
            ("topologia-runtime", "Topologia Runtime", "Topologia de procesos y servicios.", "architecture"),
            ("dependencias-servicios", "Dependencias de Servicios", "Dependencias internas y externas.", "architecture"),
            ("observabilidad", "Observabilidad", "Logs, metricas y trazas.", "architecture"),
            ("health-model", "Modelo de Salud", "Salud, readiness y degradacion.", "architecture"),
            ("resiliencia-failover", "Resiliencia y Failover", "Recuperacion y continuidad.", "architecture"),
            ("requisitos-no-funcionales", "Requisitos No Funcionales", "Latencia, disponibilidad y escalabilidad.", "architecture"),
            ("adrs", "Indice de ADRs de Arquitectura", "Acceso a decisiones tecnicas.", "architecture"),
        ],
    },
    "dominio-trading": {
        "name": "Dominio Trading",
        "pages": [
            ("index", "Indice de Dominio Trading", "Conceptos centrales del dominio.", "section-index"),
            ("vision-dominio", "Vision del Dominio", "Introduccion al dominio de trading algoritmico.", "domain"),
            ("modelo-dominio", "Modelo de Dominio", "Entidades y relaciones principales.", "domain"),
            ("semantica-ordenes", "Semantica de Ordenes", "Estados y reglas de ordenes.", "domain"),
            ("fills", "Fills", "Ejecuciones parciales y totales.", "domain"),
            ("posiciones-balances", "Posiciones y Balances", "Posiciones, balances y exposure.", "domain"),
            ("riesgo", "Riesgo", "Controles y decisiones de riesgo.", "domain"),
            ("execution-semantics", "Semantica de Ejecucion", "Reglas de ejecucion y rechazos.", "domain"),
            ("capability-matrix-exchanges", "Matriz de Capacidades de Exchanges", "Comparativa de capacidades por venue.", "domain"),
        ],
    },
    "modulos": {
        "name": "Modulos",
        "pages": [
            ("index", "Indice de Modulos", "Vista por modulos funcionales.", "section-index"),
            ("ingestion", "Modulo Ingestion", "Datos de mercado, normalizacion y replay.", "module"),
            ("exchange-adapter", "Modulo Adaptador de Exchange", "Adaptadores de APIs externas.", "module"),
            ("oms", "Modulo OMS", "Gestion del ciclo de vida de ordenes.", "module"),
            ("execution", "Modulo Ejecucion", "Envio y seguimiento de ejecuciones.", "module"),
            ("portfolio", "Modulo Portfolio", "Posiciones, balances y exposure.", "module"),
            ("risk-engine", "Modulo Motor de Riesgo", "Validaciones de riesgo.", "module"),
            ("research", "Modulo Research", "Investigacion cuantitativa.", "module"),
            ("backtesting-simulation", "Modulo Backtesting / Simulacion", "Simulacion historica.", "module"),
            ("feature-store-feature-engine", "Modulo Feature Store / Feature Engine", "Features y servicio.", "module"),
            ("control-plane-ops", "Modulo Plano de Control / Operaciones", "Control operativo y gobierno.", "module"),
        ],
    },
    "flujos": {
        "name": "Flujos y Secuencias",
        "pages": [
            ("index", "Indice de Flujos y Secuencias", "Secuencias operativas y funcionales.", "section-index"),
            ("market-data-flow", "Flujo de Datos de Mercado", "De origen a consumidores.", "flow"),
            ("order-lifecycle", "Ciclo de Vida de Ordenes", "De solicitud a cierre.", "flow"),
            ("position-balance-reconciliation", "Reconciliacion de Posiciones y Balances", "Reconciliacion de estado.", "flow"),
            ("risk-decision-flow", "Flujo de Decision de Riesgo", "Decision de riesgo previa a ejecucion.", "flow"),
            ("strategy-signal-to-execution", "Flujo de Senal de Estrategia a Ejecucion", "De senal a ejecucion.", "flow"),
            ("startup-recovery-sequence", "Secuencia de Arranque y Recuperacion", "Arranque y restauracion.", "flow"),
            ("market-data-recovery-sequence", "Secuencia de Recuperacion de Datos de Mercado", "Recovery e historico-live handoff.", "flow"),
            ("incident-recovery-flow", "Flujo de Incidente y Recuperacion", "Gestion de incidente y recuperacion.", "flow"),
        ],
    },
    "datos": {
        "name": "Datos",
        "pages": [
            ("index", "Indice de Datos", "Catalogo y contratos de datos.", "section-index"),
            ("catalogo-datos", "Catalogo de Datos", "Inventario de datasets y streams.", "data"),
            ("contratos-event-schemas", "Contratos y Esquemas de Eventos", "Payloads y compatibilidad.", "data"),
            ("data-quality-specification", "Especificacion de Calidad de Datos", "Calidad, completitud y frescura.", "data"),
            ("instrument-master", "Maestro de Instrumentos", "Fuente canonica de instrumentos.", "data"),
            ("canonical-data-model", "Modelo Canonico de Datos", "Modelo canonico compartido.", "data"),
            ("data-flow-lineage", "Flujo de Datos y Trazabilidad", "Trazabilidad end to end.", "data"),
        ],
    },
    "research-simulation": {
        "name": "Investigacion / Backtesting / Simulacion",
        "pages": [
            ("index", "Indice de Investigacion / Simulacion", "Investigacion, simulacion y promocion.", "section-index"),
            ("workflow-research", "Flujo de Trabajo de Investigacion", "Proceso desde idea a experimento.", "research"),
            ("assumptions-backtester", "Supuestos del Backtester", "Supuestos del motor de simulacion.", "research"),
            ("simulation-fidelity-matrix", "Matriz de Fidelidad de Simulacion", "Fidelidad entre simulacion y ejecucion.", "research"),
            ("promotion-path", "Ruta de Promocion", "Paso de investigacion a paper y live.", "research"),
            ("reproducibilidad-lineage", "Reproducibilidad y Trazabilidad", "Lineage de datasets, features y resultados.", "research"),
        ],
    },
    "operacion": {
        "name": "Operacion",
        "pages": [
            ("index", "Indice de Operacion", "Operacion diaria, incidentes y releases.", "section-index"),
            ("service-catalog", "Catalogo de Servicios", "Inventario de servicios operativos.", "operations"),
            ("runbooks", "Runbooks Operativos", "Indice base de procedimientos.", "runbook"),
            ("incident-playbooks", "Playbooks de Incidentes", "Respuestas para incidentes.", "runbook"),
            ("slo-sla-alerting", "SLO / SLA / Alertas", "Objetivos y alertas.", "operations"),
            ("dr-bcp", "DR / BCP", "Continuidad y recuperacion ante desastre.", "operations"),
            ("deployment-release", "Despliegue y Liberacion", "Construccion, despliegue, rollback y promocion.", "operations"),
            ("troubleshooting", "Resolucion de Problemas", "Diagnostico rapido.", "runbook"),
        ],
    },
    "desarrollo": {
        "name": "Desarrollo",
        "pages": [
            ("index", "Indice de Desarrollo", "Incorporacion, pruebas y estandares.", "section-index"),
            ("onboarding", "Incorporacion", "Primeros pasos para nuevos desarrolladores.", "development"),
            ("coding-standards", "Estandares de Codigo", "Convenciones y revisiones.", "development"),
            ("api-standards", "Estandares de Interfaces y APIs", "Contratos y compatibilidad.", "development"),
            ("testing-strategy", "Estrategia de Pruebas", "Estrategia de pruebas.", "development"),
            ("environment-configuration", "Guia de Entorno y Configuracion", "Entornos y configuracion.", "development"),
        ],
    },
    "seguridad-gobernanza": {
        "name": "Seguridad y Gobernanza",
        "pages": [
            ("index", "Indice de Seguridad y Gobernanza", "Controles de seguridad y gobierno.", "section-index"),
            ("security-architecture", "Arquitectura de Seguridad", "Modelo de seguridad de alto nivel.", "security"),
            ("secrets-credentials", "Gestion de Secretos y Credenciales", "Gestion de credenciales.", "security"),
            ("auditability-compliance", "Trazabilidad y Controles de Cumplimiento", "Trazabilidad y controles.", "security"),
            ("access-model", "Modelo de Acceso", "Roles, permisos y accesos.", "security"),
        ],
    },
    "decisiones": {
        "name": "Decisiones",
        "pages": [
            ("index", "Indice ADR", "Indice de decisiones arquitectonicas.", "section-index"),
            ("adr-0000-template", "Plantilla ADR", "Plantilla base para registrar decisiones.", "adr"),
        ],
    },
    "glosario": {
        "name": "Glosario",
        "pages": [
            ("index", "Indice Global de Glosario", "Vista consolidada del vocabulario.", "section-index"),
            ("trading", "Terminos de Trading", "Vocabulario del dominio trading.", "glossary"),
            ("aplicacion", "Terminos de Aplicacion", "Vocabulario de la plataforma.", "glossary"),
            ("desarrollo", "Terminos de Desarrollo", "Vocabulario de ingenieria.", "glossary"),
            ("performance-operacion", "Terminos de Performance y Operacion", "Vocabulario de fiabilidad y operacion.", "glossary"),
        ],
    },
    "faq": {
        "name": "Preguntas Frecuentes",
        "pages": [
            ("index", "Indice de Preguntas Frecuentes", "Preguntas frecuentes.", "section-index"),
            ("faq-general", "Preguntas Frecuentes Generales", "Preguntas comunes sobre la plataforma.", "faq"),
            ("troubleshooting-faq", "Preguntas Frecuentes de Resolucion de Problemas", "Preguntas de diagnostico rapido.", "faq"),
        ],
    },
    "postmortems": {
        "name": "Postmortems",
        "pages": [
            ("index", "Indice de Postmortems", "Registro de incidentes y aprendizajes.", "section-index"),
            ("learnings-index", "Indice de Learnings", "Contenedor para learnings operativos.", "postmortem"),
        ],
    },
    "templates": {
        "name": "Plantillas",
        "pages": [
            ("index", "Indice de Plantillas", "Plantillas para ampliar la documentacion.", "section-index"),
            ("home-template", "Plantilla de Inicio", "Plantilla de portada.", "template"),
            ("section-index-template", "Plantilla de Indice de Seccion", "Plantilla de indice de seccion.", "template"),
            ("module-template", "Plantilla de Modulo", "Plantilla de modulo funcional.", "template"),
            ("architecture-template", "Plantilla de Arquitectura", "Plantilla de arquitectura.", "template"),
            ("flow-template", "Plantilla de Flujo / Secuencia", "Plantilla de flujo.", "template"),
            ("data-template", "Plantilla de Datos / Contratos", "Plantilla de datos y contratos.", "template"),
            ("runbook-template", "Plantilla de Runbook", "Plantilla operativa.", "template"),
            ("adr-template", "Plantilla ADR", "Plantilla de decision arquitectonica.", "template"),
            ("glossary-template", "Plantilla de Glosario", "Plantilla de glosario.", "template"),
        ],
    },
}


GLOSSARY = {
    "trading": [
        ("Orden", "Instruccion para comprar o vender un instrumento."),
        ("Fill", "Ejecucion parcial o total de una orden."),
        ("Liquidez", "Capacidad del mercado para absorber volumen."),
        ("Exposicion", "Riesgo agregado derivado de posiciones abiertas."),
    ],
    "aplicacion": [
        ("Modelo Canonico", "Representacion comun usada entre modulos."),
        ("Plano de Control", "Capa de control, operacion y gobierno."),
        ("Adaptador", "Componente que traduce contratos externos al modelo interno."),
        ("Readiness", "Estado que indica si un componente esta listo para operar."),
    ],
    "desarrollo": [
        ("ADR", "Registro corto de una decision tecnica y sus consecuencias."),
        ("Prueba de Contrato", "Prueba para validar compatibilidad de interfaces."),
        ("Compatibilidad hacia Atras", "Capacidad de evolucionar sin romper consumidores."),
        ("CI", "Validacion automatizada de cambios y artefactos."),
    ],
    "performance-operacion": [
        ("SLO", "Objetivo interno medible de fiabilidad o latencia."),
        ("MTTR", "Tiempo medio de recuperacion tras un incidente."),
        ("Failover", "Cambio controlado a un camino alternativo ante fallo."),
        ("Trazabilidad", "Trazabilidad de origen y transformaciones de un dato."),
    ],
}


def all_pages() -> list[Page]:
    pages: list[Page] = []
    for section, meta in SECTIONS.items():
        for slug, title, summary, kind in meta["pages"]:
            pages.append(Page(section, slug, title, summary, kind))
    return pages


PAGES = all_pages()
PAGE_MAP = {p.url: p for p in PAGES}


RELATED_BY_KIND = {
    "architecture": [
        ("modulos/index.html", "Relaciona la vista tecnica con los modulos funcionales que la implementan."),
        ("datos/index.html", "Conecta la arquitectura con contratos, modelos y calidad de datos."),
        ("flujos/index.html", "Aterriza la arquitectura en secuencias operativas y temporales."),
        ("operacion/index.html", "Vincula la estructura del sistema con observabilidad, incidentes y releases."),
    ],
    "domain": [
        ("modulos/index.html", "Permite bajar del lenguaje del dominio a la responsabilidad de cada modulo."),
        ("flujos/index.html", "Conecta conceptos del dominio con secuencias y decisiones operativas."),
        ("datos/index.html", "Ayuda a revisar los eventos y modelos que materializan el dominio."),
        ("glosario/trading.html", "Alinea la terminologia de trading con definiciones reutilizables."),
    ],
    "module": [
        ("arquitectura/arquitectura-alto-nivel.html", "Coloca el modulo dentro de la vista estructural general."),
        ("datos/index.html", "Conecta el modulo con contratos, modelos y calidad de datos."),
        ("flujos/index.html", "Permite ver donde participa el modulo dentro de secuencias reales."),
        ("operacion/index.html", "Relaciona el modulo con operacion, observabilidad y troubleshooting."),
    ],
    "flow": [
        ("dominio-trading/index.html", "Conecta la secuencia con la semantica de negocio que la explica."),
        ("modulos/index.html", "Permite navegar a los modulos que ejecutan este flujo."),
        ("datos/index.html", "Relaciona la secuencia con contratos, eventos y lineage."),
        ("operacion/index.html", "Conecta el flujo con recuperacion, monitoreo y respuesta operativa."),
    ],
    "data": [
        ("modulos/index.html", "Relaciona modelos y contratos con los modulos productores y consumidores."),
        ("flujos/index.html", "Ayuda a entender donde aparecen estos datos en las secuencias operativas."),
        ("research-simulation/index.html", "Conecta datasets y contratos con simulacion, research y promocion."),
        ("desarrollo/api-standards.html", "Vincula los contratos de datos con estandares de interfaces y compatibilidad."),
    ],
    "research": [
        ("modulos/research.html", "Conecta el workflow cuantitativo con el modulo que lo soporta."),
        ("modulos/backtesting-simulation.html", "Relaciona supuestos y promotion path con el motor de simulacion."),
        ("datos/index.html", "Conecta reproducibilidad y simulacion con catalogo, calidad y lineage."),
        ("operacion/deployment-release.html", "Relaciona la promocion con despliegue, releases y control operativo."),
    ],
    "operations": [
        ("arquitectura/observabilidad.html", "Relaciona la operacion con metricas, logs, trazas y salud."),
        ("arquitectura/resiliencia-failover.html", "Conecta la operacion diaria con continuidad y recuperacion."),
        ("desarrollo/testing-strategy.html", "Relaciona readiness operativa con validacion tecnica previa."),
        ("decisiones/index.html", "Permite vincular cambios operativos con decisiones registradas."),
    ],
    "runbook": [
        ("operacion/service-catalog.html", "Da contexto sobre el servicio o capacidad a la que aplica el procedimiento."),
        ("operacion/troubleshooting.html", "Complementa el procedimiento con diagnostico rapido y patrones comunes."),
        ("flujos/incident-recovery-flow.html", "Relaciona el procedimiento con el flujo general de incidente y recuperacion."),
        ("postmortems/index.html", "Conecta la respuesta operativa con aprendizaje posterior e incidentes previos."),
    ],
    "development": [
        ("arquitectura/index.html", "Ayuda a entender la plataforma antes de modificarla."),
        ("datos/contratos-event-schemas.html", "Relaciona el desarrollo con contratos y compatibilidad."),
        ("operacion/deployment-release.html", "Conecta el trabajo de ingenieria con el proceso de release."),
        ("decisiones/index.html", "Permite enlazar cambios de codigo con decisiones tecnicas registradas."),
    ],
    "security": [
        ("arquitectura/contexto-sistema.html", "Coloca los controles dentro del contexto general del sistema."),
        ("operacion/deployment-release.html", "Relaciona seguridad con despliegue, secretos y cambios productivos."),
        ("desarrollo/environment-configuration.html", "Conecta controles de acceso y secretos con configuracion de entornos."),
        ("decisiones/index.html", "Permite registrar decisiones relevantes de seguridad y gobernanza."),
    ],
    "adr": [
        ("arquitectura/adrs.html", "Agrupa esta decision dentro del indice de arquitectura."),
        ("decisiones/index.html", "Relaciona la decision con el resto del historial arquitectonico."),
        ("desarrollo/coding-standards.html", "Permite bajar de la decision a su impacto en practicas de ingenieria."),
    ],
    "glossary": [
        ("glosario/index.html", "Vuelve al indice global del vocabulario compartido."),
        ("dominio-trading/index.html", "Conecta los terminos con el dominio funcional donde se usan."),
        ("modulos/index.html", "Relaciona el vocabulario con los modulos de la plataforma."),
        ("operacion/index.html", "Ayuda a vincular terminos tecnicos con su uso operativo."),
    ],
    "faq": [
        ("index.html", "Permite volver al portal para elegir una ruta de lectura mas completa."),
        ("coverage-index.html", "Ayuda a localizar que paginas siguen siendo base o placeholders."),
        ("operacion/runbooks.html", "Conecta preguntas frecuentes con procedimientos operativos."),
        ("glosario/index.html", "Aclara terminos repetidos que suelen generar dudas."),
    ],
    "postmortem": [
        ("operacion/incident-playbooks.html", "Relaciona aprendizajes con respuestas operativas estandarizadas."),
        ("flujos/incident-recovery-flow.html", "Conecta el analisis posterior con el flujo de incidente y recuperacion."),
        ("operacion/slo-sla-alerting.html", "Vincula incidentes con objetivos operativos y alertas."),
        ("decisiones/index.html", "Permite convertir aprendizajes en decisiones tecnicas visibles."),
    ],
    "template": [
        ("templates/index.html", "Permite volver al indice editorial de plantillas reutilizables."),
        ("coverage-index.html", "Ayuda a comprobar que secciones existen antes de crear o ampliar paginas."),
        ("index.html", "Vuelve al portal principal para validar donde encaja la nueva pagina."),
        ("glosario/index.html", "Sirve para mantener consistencia terminologica al rellenar la plantilla."),
    ],
}


RELATED_BY_DOC_ID = {
    "arquitectura/contexto-sistema": [
        ("arquitectura/arquitectura-alto-nivel.html", "Baja del contexto general a los bloques principales de la plataforma."),
        ("dominio-trading/modelo-dominio.html", "Complementa los limites del sistema con entidades y relaciones del dominio."),
        ("modulos/index.html", "Permite navegar del contexto general a cada responsabilidad funcional."),
        ("arquitectura/dependencias-servicios.html", "Muestra dependencias internas y externas derivadas del contexto."),
    ],
    "arquitectura/observabilidad": [
        ("operacion/slo-sla-alerting.html", "Conecta las señales tecnicas con objetivos y alertas operativas."),
        ("operacion/troubleshooting.html", "Relaciona logs y metricas con diagnostico y respuesta."),
        ("arquitectura/health-model.html", "Completa la observabilidad con estados de salud y degradacion."),
        ("operacion/incident-playbooks.html", "Permite enlazar deteccion con respuesta ante incidentes."),
    ],
    "dominio-trading/semantica-ordenes": [
        ("modulos/oms.html", "Conecta las reglas de ordenes con el modulo que gestiona su ciclo de vida."),
        ("flujos/order-lifecycle.html", "Aterriza la semantica en una secuencia temporal completa."),
        ("dominio-trading/execution-semantics.html", "Complementa estados de orden con reglas de ejecucion."),
        ("datos/contratos-event-schemas.html", "Relaciona estados y eventos con sus contratos documentales."),
    ],
    "dominio-trading/posiciones-balances": [
        ("modulos/portfolio.html", "Conecta las entidades del dominio con el modulo que consolida estado."),
        ("flujos/position-balance-reconciliation.html", "Aterriza el concepto en el flujo de reconciliacion."),
        ("dominio-trading/riesgo.html", "Relaciona posiciones y balances con exposure y controles de riesgo."),
        ("datos/canonical-data-model.html", "Conecta posiciones y balances con el modelo canonico compartido."),
    ],
    "modulos/ingestion": [
        ("flujos/market-data-flow.html", "Aterriza ingestion dentro del recorrido completo del dato de mercado."),
        ("flujos/market-data-recovery-sequence.html", "Conecta el modulo con recovery y handoff historico-live."),
        ("datos/contratos-event-schemas.html", "Relaciona ingestion con payloads, compatibilidad y modelos de eventos."),
        ("arquitectura/observabilidad.html", "Vincula el modulo con health, metricas, logs y readiness."),
    ],
    "modulos/exchange-adapter": [
        ("dominio-trading/capability-matrix-exchanges.html", "Conecta el adaptador con capacidades y limites por venue."),
        ("dominio-trading/execution-semantics.html", "Relaciona contratos externos con reglas de ejecucion."),
        ("datos/contratos-event-schemas.html", "Conecta traducciones de API con contratos internos."),
        ("modulos/execution.html", "Permite seguir el camino desde adaptacion externa hasta ejecucion."),
    ],
    "modulos/oms": [
        ("flujos/order-lifecycle.html", "Aterriza el rol del OMS en el ciclo de vida de una orden."),
        ("dominio-trading/semantica-ordenes.html", "Relaciona la gestion del OMS con estados y reglas del dominio."),
        ("modulos/execution.html", "Conecta decision y gestion de orden con su envio y seguimiento."),
        ("flujos/risk-decision-flow.html", "Permite ver el punto en que riesgo condiciona el flujo de OMS."),
    ],
    "modulos/execution": [
        ("flujos/strategy-signal-to-execution.html", "Aterriza este modulo dentro del recorrido desde senal a ejecucion."),
        ("flujos/order-lifecycle.html", "Relaciona el envio y seguimiento con el ciclo de vida de la orden."),
        ("dominio-trading/execution-semantics.html", "Conecta implementacion con reglas de ejecucion y rechazo."),
        ("modulos/exchange-adapter.html", "Permite seguir la integracion con venues y APIs externas."),
    ],
    "modulos/risk-engine": [
        ("dominio-trading/riesgo.html", "Conecta el modulo con las reglas y conceptos de riesgo del dominio."),
        ("flujos/risk-decision-flow.html", "Aterriza la intervencion del motor de riesgo dentro del flujo operativo."),
        ("modulos/portfolio.html", "Relaciona decisiones de riesgo con posiciones, balances y exposure."),
        ("operacion/slo-sla-alerting.html", "Conecta validaciones criticas con alertas y observabilidad operativa."),
    ],
    "flujos/market-data-flow": [
        ("modulos/ingestion.html", "Conecta el flujo con el modulo responsable de entrada y normalizacion."),
        ("datos/data-flow-lineage.html", "Relaciona el recorrido del dato con su trazabilidad end to end."),
        ("datos/contratos-event-schemas.html", "Permite revisar los eventos que atraviesan este flujo."),
        ("arquitectura/observabilidad.html", "Conecta el flujo con la visibilidad tecnica necesaria para operarlo."),
    ],
    "flujos/order-lifecycle": [
        ("dominio-trading/semantica-ordenes.html", "Conecta la secuencia con la semantica que define sus estados."),
        ("modulos/oms.html", "Relaciona el flujo con el modulo que orquesta el ciclo de vida."),
        ("modulos/execution.html", "Permite seguir el paso desde gestion a envio y confirmacion."),
        ("datos/contratos-event-schemas.html", "Conecta transiciones del flujo con eventos y mensajes documentados."),
    ],
    "flujos/risk-decision-flow": [
        ("dominio-trading/riesgo.html", "Conecta el flujo con controles, limites y decisiones del dominio."),
        ("modulos/risk-engine.html", "Relaciona la secuencia con el motor que evalua y decide."),
        ("flujos/strategy-signal-to-execution.html", "Permite ubicar la decision de riesgo dentro del recorrido completo."),
        ("modulos/oms.html", "Conecta la salida del flujo de riesgo con el gestor del ciclo de vida de ordenes."),
    ],
    "datos/contratos-event-schemas": [
        ("modulos/ingestion.html", "Conecta contratos de eventos con el punto de entrada de datos de mercado."),
        ("modulos/exchange-adapter.html", "Relaciona las APIs externas con el modelo interno documentado."),
        ("desarrollo/api-standards.html", "Vincula contratos de datos con estandares de interfaces y compatibilidad."),
        ("datos/canonical-data-model.html", "Permite navegar del esquema puntual al modelo compartido entre modulos."),
    ],
    "datos/canonical-data-model": [
        ("datos/contratos-event-schemas.html", "Conecta el modelo compartido con los payloads y eventos concretos."),
        ("modulos/feature-store-feature-engine.html", "Relaciona el modelo canonico con calculo y serving de features."),
        ("research-simulation/reproducibilidad-lineage.html", "Vincula el modelo comun con reproducibilidad y lineage."),
        ("glosario/aplicacion.html", "Ayuda a mantener consistencia terminologica sobre el modelo compartido."),
    ],
    "research-simulation/promotion-path": [
        ("research-simulation/workflow-research.html", "Conecta la promocion con el proceso previo de investigacion."),
        ("research-simulation/simulation-fidelity-matrix.html", "Relaciona el paso a paper o live con la fidelidad de simulacion."),
        ("operacion/deployment-release.html", "Permite conectar promocion cuantitativa con release y despliegue."),
        ("research-simulation/reproducibilidad-lineage.html", "Conecta el promotion path con trazabilidad de datasets y resultados."),
    ],
    "operacion/service-catalog": [
        ("arquitectura/topologia-runtime.html", "Relaciona el inventario operativo con procesos y topologia runtime."),
        ("operacion/runbooks.html", "Conecta cada servicio con sus procedimientos de operacion."),
        ("operacion/deployment-release.html", "Permite enlazar servicios con despliegue, rollback y promocion."),
        ("arquitectura/observabilidad.html", "Relaciona el catalogo con metricas, salud y visibilidad tecnica."),
    ],
    "operacion/troubleshooting": [
        ("operacion/runbooks.html", "Complementa el diagnostico con procedimientos paso a paso."),
        ("operacion/incident-playbooks.html", "Relaciona troubleshooting con respuesta formal a incidentes."),
        ("operacion/slo-sla-alerting.html", "Conecta sintomas y diagnostico con alertas y umbrales operativos."),
        ("arquitectura/observabilidad.html", "Permite bajar del diagnostico a logs, metricas y trazas."),
    ],
    "desarrollo/api-standards": [
        ("datos/contratos-event-schemas.html", "Conecta estandares de interfaz con contratos de eventos y payloads."),
        ("datos/canonical-data-model.html", "Relaciona APIs con el modelo compartido entre modulos."),
        ("modulos/exchange-adapter.html", "Aterriza los estandares en un modulo que traduce contratos externos."),
        ("decisiones/adr-0000-template.html", "Permite registrar cambios de interfaz que requieran una decision formal."),
    ],
    "seguridad-gobernanza/access-model": [
        ("seguridad-gobernanza/security-architecture.html", "Conecta roles y permisos con el modelo de seguridad general."),
        ("seguridad-gobernanza/secrets-credentials.html", "Relaciona acceso con gestion de credenciales y secretos."),
        ("seguridad-gobernanza/auditability-compliance.html", "Permite conectar accesos con trazabilidad y controles."),
        ("operacion/service-catalog.html", "Ayuda a mapear permisos sobre servicios y capacidades concretas."),
    ],
}


def path_ref(label: str, path: str, note: str = "") -> dict[str, str]:
    return {"type": "path", "label": label, "target": path, "note": note}


def doc_ref(label: str, target: str, note: str = "") -> dict[str, str]:
    return {"type": "doc", "label": label, "target": target, "note": note}


def note_ref(note: str) -> dict[str, str]:
    return {"type": "note", "label": "Pendiente", "target": "", "note": note}


TRUTH_SOURCES_BY_KIND = {
    "architecture": {
        "Repositorio y codigo": [
            path_ref("Especificacion tecnica base", "docs/tech_spec.md", "Documento tecnico existente en el repo."),
            path_ref("Documento base de arquitectura", "docs/architecture.md", "Referencia documental previa a este portal HTML."),
            path_ref("Codigo fuente principal", "app/", "Arbol principal de implementacion."),
            path_ref("Configuraciones de entorno", "config.dev.yaml, config.prod.yaml, config.test.yaml", "Parametros y perfiles de ejecucion."),
        ],
        "Referencias operativas": [
            doc_ref("Indice de Operacion", "operacion/index.html", "Puerta de entrada a runbooks, release e incidentes."),
            doc_ref("Catalogo de Servicios", "operacion/service-catalog.html", "Relaciona la vista tecnica con capacidades operativas."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decision log real del repo."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Indice navegable dentro del portal."),
        ],
        "Dashboards y telemetria": [
            path_ref("Codigo de observabilidad", "app/observability/", "Implementacion local verificada en el repo."),
            note_ref("No se ha verificado una URL de dashboard real en esta iteracion."),
        ],
        "Artefactos vivos": [
            note_ref("No se ha verificado un artefacto vivo transversal especifico para esta vista en esta iteracion."),
        ],
    },
    "domain": {
        "Repositorio y codigo": [
            path_ref("Definicion funcional", "docs/definition.md", "Documento base del dominio."),
            path_ref("Documento funcional", "docs/Functional.md", "Referencia funcional previa existente."),
            path_ref("Casos de uso", "docs/useCase.md", "Contexto adicional del dominio."),
            path_ref("Implementacion relacionada", "app/execution/, app/portfolio/, app/risk/, app/strategy/", "Areas de codigo vinculadas al dominio."),
        ],
        "Referencias operativas": [
            doc_ref("Runbooks Operativos", "operacion/runbooks.html", "Referencia operativa transversal."),
            doc_ref("Resolucion de Problemas", "operacion/troubleshooting.html", "Diagnostico y soporte operativo."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decisiones registradas en markdown."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso navegable a decisiones."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado un dashboard de dominio o negocio enlazable desde el repo en esta iteracion."),
        ],
        "Artefactos vivos": [
            note_ref("No se ha verificado un artefacto vivo especifico del dominio trading mas alla de los modulos y datos asociados."),
        ],
    },
    "module": {
        "Repositorio y codigo": [
            path_ref("Arbol principal de aplicacion", "app/", "Codigo fuente principal."),
            path_ref("Suite de pruebas", "tests/", "Punto de entrada a validacion automatizada."),
            path_ref("Especificacion tecnica base", "docs/tech_spec.md", "Referencia transversal del repo."),
        ],
        "Referencias operativas": [
            doc_ref("Indice de Operacion", "operacion/index.html", "Contexto operativo general."),
            doc_ref("Runbooks Operativos", "operacion/runbooks.html", "Procedimientos operativos documentados."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decisiones de diseño ya registradas."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Indice navegable del portal."),
        ],
        "Dashboards y telemetria": [
            path_ref("Codigo de observabilidad", "app/observability/", "Telemetria disponible en el codigo fuente."),
            note_ref("No se ha verificado un dashboard real enlazable para este modulo en esta iteracion."),
        ],
        "Artefactos vivos": [
            note_ref("Solo algunas capacidades tienen artefactos vivos verificados en `docs/validation/`; revisar las paginas especificas del modulo si aplica."),
        ],
    },
    "flow": {
        "Repositorio y codigo": [
            path_ref("Codigo fuente principal", "app/", "Los flujos se materializan a traves de modulos del arbol `app/`."),
            path_ref("Pruebas", "tests/", "Fuente de validacion de recorridos y contratos."),
            path_ref("Especificacion tecnica base", "docs/tech_spec.md", "Contexto transversal del pipeline."),
        ],
        "Referencias operativas": [
            doc_ref("Playbooks de Incidentes", "operacion/incident-playbooks.html", "Relaciona secuencias con respuesta operativa."),
            doc_ref("Runbooks Operativos", "operacion/runbooks.html", "Apoyo procedimental para diagnostico y recovery."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decisiones que pueden afectar el flujo."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso HTML a decisiones registradas."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado un dashboard especifico por flujo en esta iteracion."),
        ],
        "Artefactos vivos": [
            note_ref("Los artefactos vivos disponibles se concentran en validaciones y evidencias operativas de ingestion dentro de `docs/validation/`."),
        ],
    },
    "data": {
        "Repositorio y codigo": [
            path_ref("Codigo de datos de mercado", "app/marketdata/", "Implementacion de modelos, conectores y replay."),
            path_ref("Codigo de features", "app/features/", "Calculo y servicio de features."),
            path_ref("Metadata versionada", "metadata/instruments/", "Snapshots y runs de instrumentos verificados en el repo."),
        ],
        "Referencias operativas": [
            doc_ref("Estandares de Interfaces y APIs", "desarrollo/api-standards.html", "Conecta contratos con compatibilidad y cambios."),
            doc_ref("Indice de Operacion", "operacion/index.html", "Permite seguir calidad y uso operativo del dato."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decisiones aplicables a alcance y modelo de datos."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso HTML a decisiones."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado un dashboard real de calidad de datos o lineage en esta iteracion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Artefactos vivos verificados en el repo."),
            path_ref("Snapshots de instrumentos", "metadata/instruments/", "Artefactos vivos de metadata ya presentes."),
        ],
    },
    "research": {
        "Repositorio y codigo": [
            path_ref("Codigo de estrategia", "app/strategy/", "Implementacion cuantitativa y señal."),
            path_ref("Codigo de features", "app/features/", "Dependencias de research y simulacion."),
            path_ref("Pruebas cuantitativas", "tests/strategy/, tests/features/, tests/performance/", "Cobertura de research, features y rendimiento."),
        ],
        "Referencias operativas": [
            doc_ref("Despliegue y Liberacion", "operacion/deployment-release.html", "Conecta promotion path con release."),
            path_ref("Cutover operativo", "docs/ops/live_cutover.md", "Referencia operativa existente en el repo."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decisiones relevantes para promotion path y alcance."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso navegable a decisiones."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado una URL de dashboard de research o backtesting en esta iteracion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Artefactos vivos disponibles para promotion y readiness."),
        ],
    },
    "operations": {
        "Repositorio y codigo": [
            path_ref("Codigo operativo", "app/ops/", "Logica y orquestacion operativa."),
            path_ref("Codigo de observabilidad", "app/observability/", "Telemetria y visibilidad tecnica."),
            path_ref("Scripts y tooling", "scripts/", "Automatizaciones y runners auxiliares."),
        ],
        "Referencias operativas": [
            path_ref("Runbooks markdown verificados", "docs/operations/ingestion_runbook.md, docs/operations/ingestion_promotion_runbook.md, docs/operations/ingestion_rollback_checklist.md", "Runbooks reales presentes en el repo."),
            path_ref("Procedimiento de cutover", "docs/ops/live_cutover.md", "Referencia operativa real adicional."),
            doc_ref("Runbooks Operativos", "operacion/runbooks.html", "Indice HTML operativo."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decision log aplicable a operacion y promotion."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso a decisiones desde el portal."),
        ],
        "Dashboards y telemetria": [
            path_ref("Codigo de observabilidad", "app/observability/", "Fuente de verdad verificada para telemetria en codigo."),
            note_ref("No se ha verificado una URL de dashboard real en esta iteracion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Artefactos vivos operativos verificados en el repo."),
        ],
    },
    "runbook": {
        "Repositorio y codigo": [
            path_ref("Codigo operativo", "app/ops/", "Implementacion que suele respaldar estos procedimientos."),
            path_ref("Scripts y tooling", "scripts/", "Automatizaciones y helpers operativos."),
        ],
        "Referencias operativas": [
            path_ref("Runbooks markdown verificados", "docs/operations/ingestion_runbook.md, docs/operations/ingestion_promotion_runbook.md, docs/operations/ingestion_rollback_checklist.md", "Procedimientos reales presentes en el repo."),
            path_ref("Procedimiento de cutover", "docs/ops/live_cutover.md", "Referencia operativa complementaria."),
            doc_ref("Playbooks de Incidentes", "operacion/incident-playbooks.html", "Relaciona el procedimiento con incident response."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decisiones que pueden justificar el procedimiento."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso navegable a decisiones."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado una URL de dashboard real asociada a este procedimiento en esta iteracion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Artefactos vivos operativos disponibles."),
        ],
    },
    "development": {
        "Repositorio y codigo": [
            path_ref("Bootstrap del repo", "README.md", "Entrada principal para uso local."),
            path_ref("Toolchain y dependencias", "pyproject.toml, poetry.lock", "Definicion del entorno Python."),
            path_ref("Pruebas y automatizacion", "pytest.ini, Makefile, scripts/", "Ejecucion y validacion de cambios."),
            path_ref("Contenerizacion", "Dockerfile, docker-compose.yml", "Entorno reproducible para pruebas y ejecucion."),
        ],
        "Referencias operativas": [
            doc_ref("Despliegue y Liberacion", "operacion/deployment-release.html", "Conecta desarrollo con release."),
            doc_ref("Runbooks Operativos", "operacion/runbooks.html", "Referencia operacional complementaria."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Decision log tecnico."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso desde el portal."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado una URL de dashboard de CI, calidad o performance en esta iteracion."),
        ],
        "Artefactos vivos": [
            path_ref("Suite de pruebas", "tests/", "Artefacto vivo de validacion continua."),
        ],
    },
    "security": {
        "Repositorio y codigo": [
            path_ref("Configuracion sensible y ejemplos", ".env.example, config.dev.yaml, config.prod.yaml, config.test.yaml", "Fuentes parciales verificadas en el repo."),
            path_ref("Contenerizacion", "Dockerfile, docker-compose.yml", "Superficie tecnica relevante para seguridad operativa."),
        ],
        "Referencias operativas": [
            doc_ref("Despliegue y Liberacion", "operacion/deployment-release.html", "Conecta controles con cambios productivos."),
            doc_ref("Catalogo de Servicios", "operacion/service-catalog.html", "Da contexto sobre superficies y responsabilidades."),
        ],
        "ADRs y decisiones": [
            path_ref("Repositorio de ADRs", "docs/adr/", "No hay una carpeta dedicada de seguridad; usar ADRs como decision log visible."),
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Acceso navegable a decisiones."),
        ],
        "Dashboards y telemetria": [
            note_ref("No se ha verificado un dashboard real de seguridad o compliance en esta iteracion."),
        ],
        "Artefactos vivos": [
            note_ref("No se ha verificado un artefacto vivo especifico de seguridad mas alla de configuraciones y decision logs."),
        ],
    },
    "adr": {
        "Repositorio y codigo": [
            path_ref("Repositorio de ADRs", "docs/adr/", "Fuente de verdad markdown para decisiones registradas."),
            path_ref("Documento tecnico base", "docs/tech_spec.md", "Contexto tecnico transversal del repo."),
        ],
        "Referencias operativas": [
            doc_ref("Indice ADR HTML", "decisiones/index.html", "Indice HTML del historial de decisiones."),
            doc_ref("ADRs de Arquitectura", "arquitectura/adrs.html", "Vista de decisiones desde la seccion de arquitectura."),
        ],
        "ADRs y decisiones": [
            path_ref("ADR-0001", "docs/adr/ADR-0001-historical-market-data-scope.md", "Decision real presente en el repo."),
            path_ref("ADR-0002", "docs/adr/ADR-0002-raw-trade-history-scope.md", "Decision real presente en el repo."),
        ],
        "Dashboards y telemetria": [
            note_ref("No aplica una fuente de dashboard real verificada para esta pagina en esta iteracion."),
        ],
        "Artefactos vivos": [
            note_ref("Los ADRs son historico vivo de decisiones; no se ha verificado otro artefacto operativo adicional en esta iteracion."),
        ],
    },
}


TRUTH_SOURCES_BY_DOC_ID = {
    "modulos/ingestion": {
        "Repositorio y codigo": [
            path_ref("Codigo de ingestion", "app/ingestion/, app/marketdata/", "Fuente de verdad principal del modulo."),
            path_ref("Pruebas de ingestion", "tests/ingestion/, tests/marketdata/", "Cobertura automatizada verificada en el repo."),
        ],
        "Referencias operativas": [
            path_ref("Runbooks de ingestion", "docs/operations/ingestion_runbook.md, docs/operations/ingestion_promotion_runbook.md, docs/operations/ingestion_rollback_checklist.md", "Documentacion operativa real de ingestion."),
        ],
        "ADRs y decisiones": [
            path_ref("ADR-0001", "docs/adr/ADR-0001-historical-market-data-scope.md", "Scope historico de market data."),
            path_ref("ADR-0002", "docs/adr/ADR-0002-raw-trade-history-scope.md", "Scope de raw trade history."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de ingestion", "docs/validation/", "Readiness, release gates, canary, soak y benchmarks verificados en el repo."),
            path_ref("Snapshots de instrumentos", "metadata/instruments/", "Artefactos vivos usados por ingestion y normalizacion."),
        ],
    },
    "modulos/feature-store-feature-engine": {
        "Repositorio y codigo": [
            path_ref("Codigo de features", "app/features/", "Fuente de verdad del feature store / engine."),
            path_ref("Pruebas de features", "tests/features/", "Cobertura verificada en el repo."),
        ],
    },
    "modulos/control-plane-ops": {
        "Repositorio y codigo": [
            path_ref("Codigo de control y ops", "app/ops/, app/observability/", "Implementacion operativa y de observabilidad."),
            path_ref("Pruebas operativas", "tests/ops/", "Cobertura verificada para operaciones."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias operativas", "docs/validation/", "Artefactos de readiness y promotion presentes en el repo."),
        ],
    },
    "flujos/market-data-flow": {
        "Repositorio y codigo": [
            path_ref("Implementacion del flujo", "app/ingestion/, app/marketdata/", "Codigo que soporta este recorrido."),
            path_ref("Pruebas del flujo", "tests/ingestion/, tests/marketdata/", "Validacion automatizada asociada."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias del flujo de ingestion", "docs/validation/", "Canary, soak, parity y release gates disponibles."),
        ],
    },
    "flujos/market-data-recovery-sequence": {
        "Repositorio y codigo": [
            path_ref("Recovery de market data", "app/ingestion/, app/marketdata/, app/ops/", "Codigo relacionado con recovery y handoff."),
        ],
        "Referencias operativas": [
            path_ref("Runbooks de ingestion", "docs/operations/ingestion_runbook.md, docs/operations/ingestion_rollback_checklist.md", "Procedimientos relevantes para recovery."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de recovery", "docs/validation/", "Artefactos vivos verificados de canary, soak y drill."),
        ],
    },
    "datos/contratos-event-schemas": {
        "Repositorio y codigo": [
            path_ref("Contratos y modelos", "app/ingestion/, app/marketdata/, app/features/", "Codigo fuente donde viven payloads y normalizacion."),
        ],
        "Artefactos vivos": [
            path_ref("Vendor contracts y parity", "docs/validation/", "Artefactos vivos de contratos y paridad verificados en el repo."),
        ],
    },
    "datos/instrument-master": {
        "Repositorio y codigo": [
            path_ref("Catalogo de instrumentos", "app/marketdata/, metadata/instruments/", "Codigo y snapshots reales del instrument master."),
        ],
        "Artefactos vivos": [
            path_ref("Snapshots actuales", "metadata/instruments/env=dev/venue=BINANCE/latest.json", "Artefacto vivo verificado en el repo."),
        ],
    },
    "research-simulation/promotion-path": {
        "Referencias operativas": [
            path_ref("Cutover operativo", "docs/ops/live_cutover.md", "Referencia existente para transicion y operacion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de promotion", "docs/validation/", "Readiness y release gates verificados en el repo."),
        ],
    },
    "operacion/service-catalog": {
        "Referencias operativas": [
            path_ref("Runbooks de ingestion", "docs/operations/ingestion_runbook.md, docs/operations/ingestion_promotion_runbook.md, docs/operations/ingestion_rollback_checklist.md", "Servicios con cobertura operativa verificada en el repo."),
            path_ref("Live cutover", "docs/ops/live_cutover.md", "Referencia operativa adicional."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias operativas", "docs/validation/", "Artefactos vivos presentes en el repo."),
        ],
    },
    "operacion/deployment-release": {
        "Referencias operativas": [
            path_ref("Live cutover", "docs/ops/live_cutover.md", "Procedimiento operativo real verificado."),
            path_ref("Runbook de promotion", "docs/operations/ingestion_promotion_runbook.md", "Runbook real de promotion."),
        ],
        "Artefactos vivos": [
            path_ref("Release gates", "docs/validation/", "Artefactos vivos de promotion y readiness."),
        ],
    },
    "operacion/troubleshooting": {
        "Referencias operativas": [
            path_ref("Runbook de ingestion", "docs/operations/ingestion_runbook.md", "Procedimiento real disponible."),
            path_ref("Rollback checklist", "docs/operations/ingestion_rollback_checklist.md", "Checklist real verificada en el repo."),
        ],
    },
    "decisiones/adr-0000-template": {
        "ADRs y decisiones": [
            path_ref("ADR-0001", "docs/adr/ADR-0001-historical-market-data-scope.md", "Ejemplo real de decision ya registrada."),
            path_ref("ADR-0002", "docs/adr/ADR-0002-raw-trade-history-scope.md", "Ejemplo real de decision ya registrada."),
        ],
    },
}


def href(current: Page, target: str) -> str:
    base = current.path.parent
    target_path = DOCS / target
    return Path(target_path.relative_to(base) if target_path.is_relative_to(base) else Path()).as_posix()


def rel_href(current: Page, target: str) -> str:
    return Path(Path(target).relative_to(".")).as_posix() if current.url == target else Path(
        Path(Path(current.url).parent).relative_to(".")
    ).joinpath("..")


def link_from(current: Page, target: str) -> str:
    return Path(
        Path(target).relative_to(Path(current.url).parent) if Path(target).is_relative_to(Path(current.url).parent) else ""
    ).as_posix()


def relative_link(current: Page, target: str) -> str:
    current_dir = Path(current.url).parent
    return Path(Path(target)).relative_to(current_dir).as_posix() if Path(target).is_relative_to(current_dir) else str(
        Path(*([".."] * len(current_dir.parts)), target).as_posix()
    )


def relpath(current: Page, target: str) -> str:
    import os

    return os.path.relpath(DOCS / target, current.path.parent).replace("\\", "/")


def nav(current: Page) -> str:
    items = []
    for section, meta in SECTIONS.items():
        target = "index.html" if not section else f"{section}/index.html"
        active = " active" if current.url == target else ""
        items.append(f'<a class="nav-link{active}" href="{escape(relpath(current, target))}">{escape(meta["name"])}</a>')
    return "\n".join(items)


def breadcrumbs(current: Page) -> str:
    crumbs = [f'<a href="{escape(relpath(current, "index.html"))}">Inicio</a>']
    if current.section:
        section_target = f"{current.section}/index.html"
        crumbs.append(f'<a href="{escape(relpath(current, section_target))}">{escape(SECTIONS[current.section]["name"])}</a>')
    if current.slug != "index":
        crumbs.append(f"<span>{escape(current.title)}</span>")
    return " / ".join(crumbs)


def section_page_links(current: Page) -> str:
    if current.kind != "section-index":
        return ""
    rows = []
    for page in PAGES:
        if page.section == current.section and page.slug != "index":
            rows.append(
                "<li>"
                f'<a href="{escape(relpath(current, page.url))}">{escape(page.title)}</a>'
                '<span class="status-pill">Base creada</span>'
                "</li>"
            )
    return "<section><h2>Paginas incluidas</h2><ul class=\"link-list\">%s</ul></section>" % "".join(rows)


def mermaid_diagrams_dir() -> Path:
    return DOCS / "assets" / MERMAID_DIAGRAMS_DIRNAME


def mermaid_diagram_filename(page: Page) -> str:
    section = page.section or "portal"
    return f"{section}--{page.slug}.svg"


def mermaid_diagram_relative_path(page: Page) -> str:
    return f"assets/{MERMAID_DIAGRAMS_DIRNAME}/{mermaid_diagram_filename(page)}"


def local_browser_path() -> Path | None:
    for browser_path in BROWSER_CANDIDATES:
        if browser_path.exists():
            return browser_path
    return None


def mermaid_render_html(mermaid: str) -> str:
    script_uri = MERMAID_VENDOR_SOURCE.resolve().as_uri()
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <style>
    html, body {{
      margin: 0;
      background: #ffffff;
      overflow: hidden;
    }}
    body {{
      padding: 24px;
      display: inline-block;
    }}
    #diagram {{
      display: inline-block;
    }}
    .mermaid svg {{
      max-width: 1500px;
      height: auto;
    }}
  </style>
</head>
<body>
  <div id="diagram">
    <div class="mermaid">{escape(mermaid)}</div>
  </div>
  <script src="{script_uri}"></script>
  <script>
    mermaid.initialize({{
      startOnLoad: false,
      theme: "neutral",
      securityLevel: "loose",
      flowchart: {{ curve: "basis" }}
    }});
    mermaid.run({{ querySelector: ".mermaid" }}).then(() => {{
      document.body.setAttribute("data-rendered", "true");
    }});
  </script>
</body>
</html>
"""


def render_mermaid_svg(mermaid: str) -> str | None:
    browser = local_browser_path()
    if browser is None or not MERMAID_VENDOR_SOURCE.exists():
        return None
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        html_path = temp_dir / "mermaid-render.html"
        html_path.write_text(mermaid_render_html(mermaid), encoding="utf-8")
        command = [
            str(browser),
            "--headless=new",
            "--disable-gpu",
            "--virtual-time-budget=4000",
            "--dump-dom",
            html_path.as_uri(),
        ]
        try:
            result = subprocess.run(command, check=True, timeout=60, capture_output=True, text=True)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return None
        match = re.search(r"(<svg[\s\S]*?</svg>)", result.stdout)
        if not match:
            return None
        return match.group(1)


def mermaid_for(page: Page) -> str:
    if page.doc_id == "arquitectura/contexto-sistema":
        return """
flowchart LR
  Operador["Operador"] --> Sistema["MyTraderSystem"]
  Quant["Desarrollador y Quant"] --> Sistema
  Binance["Binance REST y WS"] --> Sistema
  Storage["data_dir y metadata local"] <--> Sistema
  Sistema --> Artefactos["Logs y artefactos operativos"]
"""
    if page.doc_id == "arquitectura/arquitectura-alto-nivel":
        return """
flowchart LR
  Config["Config y CLI"] --> Ingestion["Ingestion y Market Data"]
  Binance["Binance REST y WS"] --> Ingestion
  Metadata["Metadata de instrumentos"] --> Ingestion
  Ingestion --> Features["Feature Pipeline"]
  Features --> Strategy["Strategy"]
  Strategy --> Risk["Risk"]
  Risk --> Execution["Execution paper"]
  Execution --> Portfolio["Portfolio"]
  Ingestion --> Storage["Parquet y checkpoints"]
  Ops["Ops y Observability"] --> Ingestion
  Ops --> Features
  Ops --> Execution
"""
    if page.kind == "section-index":
        return """
flowchart TD
  A["Portal documental"] --> B["Arquitectura"]
  A --> C["Dominio Trading"]
  A --> D["Modulos"]
  A --> E["Flujos"]
  A --> F["Datos"]
  A --> G["Operacion y Desarrollo"]
"""
    if page.kind == "architecture":
        return """
flowchart LR
  Usuario["Usuarios / Operadores"] --> Plataforma["Plataforma"]
  Plataforma --> Servicios["Servicios internos"]
  Plataforma --> Exchanges["Exchanges / Brokers"]
  Plataforma --> Obs["Observabilidad"]
"""
    if page.kind == "domain":
        return """
flowchart LR
  Senal["Senal"] --> Orden["Orden"]
  Orden --> Fill["Fill"]
  Fill --> Posicion["Posicion"]
  Posicion --> Balance["Balance"]
"""
    if page.kind == "module":
        return f"""
flowchart LR
  Entrada["Entradas"] --> Modulo["{escape(page.title)}"]
  Modulo --> Salida["Salidas / contratos"]
  Modulo --> Obs["Metricas / logs"]
"""
    if page.kind == "flow":
        return """
sequenceDiagram
  participant A as Actor origen
  participant B as Modulo intermedio
  participant C as Servicio destino
  A->>B: Evento / solicitud
  B->>C: Proceso / decision
  C-->>B: Resultado
"""
    if page.kind == "data":
        return """
flowchart LR
  Fuente["Fuente"] --> Raw["Raw / landing"]
  Raw --> Canonico["Modelo canonico"]
  Canonico --> Consumidores["Consumidores"]
"""
    if page.kind == "research":
        return """
flowchart LR
  Idea["Idea"] --> Research["Investigacion"]
  Research --> Backtest["Backtesting"]
  Backtest --> Paper["Paper"]
  Paper --> Live["Live"]
"""
    if page.kind in {"operations", "runbook"}:
        return """
flowchart LR
  Alerta["Alerta / trigger"] --> Triage["Triage"]
  Triage --> Accion["Accion operativa"]
  Accion --> Verificacion["Verificacion"]
  Verificacion --> Escalado["Escalado / cierre"]
"""
    if page.kind == "development":
        return """
flowchart LR
  Dev["Desarrollador"] --> Codigo["Codigo"]
  Codigo --> Tests["Tests"]
  Tests --> Review["Review"]
  Review --> Release["Release"]
"""
    if page.kind == "security":
        return """
flowchart LR
  Identidad["Identidad"] --> Acceso["Acceso"]
  Acceso --> Servicio["Servicio"]
  Servicio --> Auditoria["Auditoria"]
"""
    return ""


def related_links_section(page: Page) -> str:
    if page.kind in {"home", "coverage", "section-index"}:
        return ""

    candidates = []
    candidates.extend(RELATED_BY_DOC_ID.get(page.doc_id, []))
    candidates.extend(RELATED_BY_KIND.get(page.kind, []))

    seen: set[str] = set()
    items = []
    for target, reason in candidates:
        if target == page.url or target in seen or target not in PAGE_MAP:
            continue
        seen.add(target)
        related_page = PAGE_MAP[target]
        section_name = SECTIONS[related_page.section]["name"] if related_page.section else "Inicio"
        items.append(
            "<li>"
            "<div class=\"related-link-body\">"
            f"<p class=\"related-link-meta\">{escape(section_name)}</p>"
            f'<a class="related-link-title" href="{escape(relpath(page, target))}">{escape(related_page.title)}</a>'
            f"<p class=\"related-link-copy\">{escape(reason)}</p>"
            "</div>"
            "</li>"
        )

    if not items:
        return ""

    return (
        "<section><h2>Enlaces cruzados recomendados</h2>"
        "<p>Usa estas referencias para completar el contexto de esta pagina desde arquitectura, dominio, datos, flujos u operacion, segun corresponda.</p>"
        f"<ul class=\"related-links\">{''.join(items)}</ul></section>"
    )


def truth_sources_section(page: Page) -> str:
    if page.kind in {"home", "coverage", "section-index", "glossary", "faq", "postmortem", "template"}:
        return ""

    merged: dict[str, list[dict[str, str]]] = {}
    for source_map in (TRUTH_SOURCES_BY_KIND.get(page.kind, {}), TRUTH_SOURCES_BY_DOC_ID.get(page.doc_id, {})):
        for heading, refs in source_map.items():
            merged.setdefault(heading, [])
            merged[heading].extend(refs)

    if not merged:
        return ""

    cards = []
    for heading, refs in merged.items():
        items = []
        seen: set[tuple[str, str, str]] = set()
        for ref in refs:
            key = (ref["type"], ref["label"], ref["target"])
            if key in seen:
                continue
            seen.add(key)
            note_html = f'<p class="truth-note">{escape(ref["note"])}</p>' if ref.get("note") else ""
            if ref["type"] == "doc" and ref["target"] in PAGE_MAP:
                body = (
                    f'<a class="truth-link" href="{escape(relpath(page, ref["target"]))}">{escape(ref["label"])}</a>'
                    f"{note_html}"
                )
            elif ref["type"] == "path":
                body = (
                    f'<p class="truth-path-label">{escape(ref["label"])}</p>'
                    f'<code class="truth-path">{escape(ref["target"])}</code>'
                    f"{note_html}"
                )
            else:
                body = (
                    '<p class="truth-path-label">Pendiente de enlace verificado</p>'
                    f'<p class="truth-note truth-note-pending">{escape(ref["note"])}</p>'
                )
            items.append(f'<li>{body}</li>')

        cards.append(
            "<article class=\"truth-card\">"
            f"<h3>{escape(heading)}</h3>"
            f"<ul class=\"truth-list\">{''.join(items)}</ul>"
            "</article>"
        )

    return (
        "<section><h2>Fuentes de verdad y referencias operativas</h2>"
        "<p>Este bloque resume las referencias verificadas en el repo y en el portal para ampliar o validar esta pagina. Cuando no existe una fuente enlazable verificada, el hueco se deja marcado como pendiente.</p>"
        f"<div class=\"truth-grid\">{''.join(cards)}</div>"
        "</section>"
    )


def page_specific_sections(page: Page) -> str:
    if page.doc_id == "arquitectura/contexto-sistema":
        return """
<section><h2>Resumen del sistema</h2><p>El repo implementa una plataforma cuantitativa con un punto de entrada principal en <code>app.main</code> y un servicio de ingestion aislable en <code>app.ingestion.service.run_ingestion_service(...)</code>. La plataforma captura market data, calcula features, genera senales, aplica riesgo, ejecuta la ruta paper visible en el codigo y actualiza el estado de portfolio.</p></section>
<section><h2>Actores y sistemas externos</h2><table><thead><tr><th>Actor o sistema</th><th>Rol verificado en el repo</th><th>Relacion con la plataforma</th></tr></thead><tbody><tr><td>Operador</td><td>Ejecuta modos dry, paper o live, revisa artefactos y usa runbooks.</td><td>Inicia procesos, valida readiness y actua ante incidencias.</td></tr><tr><td>Desarrollador y quant</td><td>Modifica features, estrategia, riesgo, contratos y configuracion.</td><td>Evoluciona el codigo y valida cambios con tests y tooling local.</td></tr><tr><td>Binance REST y WS</td><td>Proveedor externo verificado para market data y metadata de instrumentos.</td><td>Entrega feeds historicos y live consumidos por ingestion.</td></tr><tr><td>Filesystem local</td><td>Superficie de persistencia visible en el repo.</td><td>Almacena Parquet, checkpoints, snapshots de instrumentos y artefactos de validacion.</td></tr></tbody></table></section>
<section><h2>Limites del sistema</h2><table><thead><tr><th>Dentro del limite</th><th>Fuera del limite</th></tr></thead><tbody><tr><td><ul><li><code>app.config</code> y CLI principal</li><li><code>app.ingestion</code> y <code>app.marketdata</code></li><li><code>app.features</code></li><li><code>app.strategy</code>, <code>app.risk</code>, <code>app.execution</code> y <code>app.portfolio</code></li><li><code>app.ops</code> y <code>app.observability</code></li></ul></td><td><ul><li>Proveedor externo Binance</li><li>Entorno de ejecucion y filesystem donde se persisten datos</li><li>Dashboards reales no verificados en esta iteracion</li><li>Servicios externos adicionales no documentados en el repo revisado</li></ul></td></tr></tbody></table></section>
<section><h2>Responsabilidades del contenedor principal</h2><ul><li><code>app.main</code> resuelve modo, carga configuracion, aplica controles operativos y coordina el pipeline cuantitativo.</li><li><code>app.ingestion.service</code> permite ejecutar la captura de market data como capacidad aislada del resto del ciclo de trading.</li><li><code>app.features.pipeline</code> y <code>app.features.engine</code> convierten eventos en <code>FeatureVector</code> reutilizable por strategy y risk.</li><li><code>app.execution.paper</code> y <code>app.portfolio.state</code> materializan la ruta de ejecucion y estado visible en el repo revisado.</li></ul></section>
<section><h2>Informacion pendiente</h2><ul><li>No se ha verificado en esta iteracion una topologia de despliegue multi proceso o multi servicio fuera del repo local.</li><li>No se ha verificado un adaptador de ejecucion live externo equivalente a la ruta <code>paper_execute</code> visible en <code>app.execution</code>.</li><li>No se ha verificado una URL de dashboard o plataforma de observabilidad operativa concreta.</li></ul></section>
"""
    if page.doc_id == "arquitectura/arquitectura-alto-nivel":
        return """
<section><h2>Lectura de alto nivel</h2><p>La arquitectura verificada en el repo esta organizada como un pipeline cuantitativo por capas. El flujo dominante entra por ingestion y market data, pasa por feature engineering, estrategia y riesgo, y termina en la capa de ejecucion y portfolio. La observabilidad y la logica operativa actuan como capacidad transversal.</p></section>
<section><h2>Bloques principales</h2><table><thead><tr><th>Bloque</th><th>Paquetes o modulos visibles</th><th>Responsabilidad principal</th></tr></thead><tbody><tr><td>Configuracion y arranque</td><td><code>app.config</code>, <code>app.main</code>, <code>app.__main__</code></td><td>Cargar configuracion, resolver modo, validar opciones y arrancar el pipeline.</td></tr><tr><td>Ingestion y market data</td><td><code>app.ingestion</code>, <code>app.marketdata</code></td><td>Capturar, validar, normalizar, deduplicar, persistir y recuperar market data.</td></tr><tr><td>Feature pipeline</td><td><code>app.features</code></td><td>Construir features, mantener estado, cache, lineage y checks asociados.</td></tr><tr><td>Decision cuantitativa</td><td><code>app.strategy</code>, <code>app.risk</code></td><td>Transformar <code>FeatureVector</code> en senales y despues en intenciones filtradas por riesgo.</td></tr><tr><td>Ejecucion y estado</td><td><code>app.execution</code>, <code>app.portfolio</code></td><td>Materializar la ejecucion paper verificada y actualizar posiciones o cash.</td></tr><tr><td>Ops y observabilidad</td><td><code>app.ops</code>, <code>app.observability</code></td><td>Emitir logs, gates, readiness, evidencias operativas y soporte de promocion.</td></tr></tbody></table></section>
<section><h2>Cadena principal de procesamiento</h2><table><thead><tr><th>Paso</th><th>Entrada visible</th><th>Salida visible</th></tr></thead><tbody><tr><td>1. Ingestion</td><td>WS o REST del venue y metadata de instrumentos</td><td><code>IngestionEvent</code> o eventos normalizados persistidos</td></tr><tr><td>2. Features</td><td>Eventos aceptados por ingestion</td><td><code>FeatureVector</code></td></tr><tr><td>3. Strategy</td><td><code>FeatureVector</code></td><td>Signals</td></tr><tr><td>4. Risk</td><td>Signals y mapa de precios</td><td><code>OrderIntent</code></td></tr><tr><td>5. Execution</td><td><code>OrderIntent</code></td><td><code>ExecutionReport</code></td></tr><tr><td>6. Portfolio</td><td><code>ExecutionReport</code></td><td><code>PortfolioState</code></td></tr></tbody></table></section>
<section><h2>Responsabilidades y limites por bloque</h2><ul><li>La capa de ingestion esta desacoplada para poder ejecutarse sola y para soportar validacion, replay y persistencia.</li><li>La capa de features concentra el estado cuantitativo reutilizable por research y runtime.</li><li>La capa de decision separa generacion de senal y control de riesgo antes de llegar a ejecucion.</li><li>La capa de ejecucion visible en el repo revisado esta representada por la ruta paper; no se documenta aqui un ejecutor live no verificado.</li><li>Ops y observabilidad no son un modulo de negocio, sino una capacidad transversal sobre ingestion, features y promotion.</li></ul></section>
<section><h2>Huecos conocidos</h2><ul><li>No se ha verificado un message bus, cola o scheduler dedicado entre capas; el repo revisado expone principalmente una orquestacion por proceso y paquetes Python.</li><li>No se ha verificado una persistencia transaccional adicional para portfolio, ordenes o auditoria fuera de filesystem y artefactos locales visibles.</li><li>No se ha verificado una topologia de despliegue distribuida; esa vista debe completarse en las paginas de deployment y topologia runtime.</li></ul></section>
"""
    return ""


def home_visual(page: Page) -> str:
    if page.kind != "home":
        return ""
    return (
        "<section class=\"home-diagram\">"
        "<div class=\"section-heading\">"
        "<p class=\"section-kicker\">Mapa de navegacion</p>"
        "<h2>Arquitectura documental inicial</h2>"
        "<p>La portada ya no muestra un Mermaid generico. Usa la imagen adjunta como vista principal del portal y deja el mapa visual listo para futuras iteraciones.</p>"
        "</div>"
        "<figure class=\"diagram-figure\">"
        f"<img src=\"{escape(relpath(page, f'assets/{HOME_DIAGRAM_NAME}'))}\" alt=\"Mapa visual del portal documental\" />"
        "<figcaption>Vista base del portal documental. Sustituir solo cuando exista una version aprobada mas reciente.</figcaption>"
        "</figure>"
        "</section>"
    )


def generic_sections(page: Page) -> str:
    purpose = {
        "home": "Base documental navegable, profesional e incremental para entender y evolucionar la plataforma.",
        "coverage": "Controlar el grado de completitud de la documentacion y sus placeholders activos.",
        "section-index": f"Organizar la seccion {SECTIONS[page.section]['name']} y sus paginas hijas.",
        "module": "Definir responsabilidad, contratos, limites y extensibilidad del modulo.",
        "architecture": "Documentar vistas estructurales, dependencias y restricciones no funcionales.",
        "domain": "Fijar semantica comun del dominio y evitar ambiguedades.",
        "flow": "Modelar secuencias clave con orden temporal claro.",
        "data": "Alinear productores y consumidores sobre contratos, calidad y lineage.",
        "research": "Hacer explicitos los supuestos entre research, simulacion y produccion.",
        "operations": "Guiar operacion, releases e incidentes.",
        "runbook": "Servir como procedimiento reutilizable y verificable.",
        "development": "Reducir tiempo de incorporacion y alinear practicas de ingenieria.",
        "security": "Explicar controles, accesos, secretos y trazabilidad.",
        "adr": "Mantener un formato uniforme para decisiones tecnicas.",
        "glossary": "Mantener un vocabulario enlazable y consistente.",
        "faq": "Responder preguntas repetidas con rapidez.",
        "postmortem": "Registrar aprendizajes estructurados tras incidentes.",
        "template": "Reutilizar estructura sin rehacer el modelo documental.",
    }[page.kind]
    blocks = [
        "<section><h2>Proposito</h2><p>%s</p></section>" % escape(purpose),
        "<section><h2>Contenido inicial</h2><ul>"
        "<li>Explicar el alcance real de esta pagina.</li>"
        "<li>Identificar las fuentes de verdad que deberian alimentarla.</li>"
        "<li>Enlazar con modulos, datos, flujos u operacion cuando corresponda.</li>"
        "</ul></section>",
        page_specific_sections(page),
        related_links_section(page),
        truth_sources_section(page),
        "<section><h2>Placeholders estructurados</h2><ul>"
        "<li>Completar detalles reales de la plataforma sin inventar implementacion no verificada.</li>"
        "<li>Anadir enlaces a codigo, dashboards, runbooks o ADRs reales cuando existan.</li>"
        "<li>Confirmar responsable de mantenimiento de esta pagina.</li>"
        "</ul></section>",
    ]
    if page.kind == "glossary":
        blocks.append(glossary_table(page))
    if page.kind == "faq":
        blocks.append(
            "<section><h2>Preguntas iniciales</h2><dl class=\"faq-list\">"
            "<dt>Por donde empezar?</dt><dd>Inicio, Arquitectura, Dominio Trading y Glosario.</dd>"
            "<dt>Que paginas requieren mas trabajo?</dt><dd>Revisa la matriz de cobertura documental.</dd>"
            "<dt>Donde registrar decisiones nuevas?</dt><dd>En Decisiones mediante ADRs.</dd>"
            "</dl></section>"
        )
    if page.kind == "postmortem":
        blocks.append(
            "<section><h2>Estructura sugerida</h2><ol>"
            "<li>Resumen ejecutivo.</li><li>Impacto y alcance.</li><li>Timeline factual.</li>"
            "<li>Causa raiz.</li><li>Remediaciones.</li><li>Follow-ups.</li>"
            "</ol></section>"
        )
    if page.kind == "adr":
        blocks.append(
            "<section><h2>Plantilla ADR</h2><p class=\"placeholder\">Contexto, decision, alternativas, consecuencias y estado.</p></section>"
        )
    if page.kind == "template":
        blocks.append(
            "<section><h2>Uso de la plantilla</h2><p>Duplicar, renombrar, mantener metadatos y sustituir placeholders por informacion validada.</p></section>"
        )
    if page.kind == "runbook":
        blocks.append(
            "<section><h2>Checklist base</h2><ol>"
            "<li>Objetivo.</li><li>Prerequisitos.</li><li>Pasos de ejecucion.</li>"
            "<li>Verificacion.</li><li>Rollback o escalado.</li><li>Artefactos.</li>"
            "</ol></section>"
        )
    if page.kind == "coverage":
        blocks.append(coverage_table(page))
    return "\n".join(blocks)


def glossary_table(page: Page) -> str:
    rows = []
    for term, definition in GLOSSARY.get(page.slug, []):
        rows.append(f"<tr><td>{escape(term)}</td><td>{escape(definition)}</td></tr>")
    return (
        "<section><h2>Entradas iniciales</h2><table><thead><tr><th>Termino</th><th>Definicion</th></tr></thead>"
        f"<tbody>{''.join(rows) if rows else '<tr><td colspan=\"2\">Pendiente</td></tr>'}</tbody></table></section>"
    )


def coverage_table(page: Page) -> str:
    rows = []
    for item in PAGES:
        rows.append(
            "<tr>"
            f"<td>{escape(SECTIONS[item.section]['name'] if item.section else 'Inicio')}</td>"
            f'<td><a href="{escape(relpath(page, item.url))}">{escape(item.title)}</a></td>'
            f"<td>{escape(item.kind)}</td><td>Base creada</td><td>Si</td></tr>"
        )
    return (
        "<section><h2>Matriz global</h2><table id=\"coverage-table\">"
        "<thead><tr><th>Seccion</th><th>Pagina</th><th>Tipo</th><th>Estado</th><th>Placeholders</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></section>"
    )


def home_cards(page: Page) -> str:
    if page.kind != "home":
        return ""
    cards = []
    for section, meta in SECTIONS.items():
        if not section:
            continue
        cards.append(
            "<article class=\"card\">"
            f"<h3>{escape(meta['name'])}</h3>"
            f"<p>{escape(meta['pages'][0][2])}</p>"
            f'<a class="button" href="{escape(relpath(page, f"{section}/index.html"))}">Abrir seccion</a>'
            "</article>"
        )
    return "<section><h2>Secciones principales</h2><div class=\"card-grid\">%s</div></section>" % "".join(cards)


def hero_content(page: Page) -> str:
    if page.kind != "home":
        return (
            f"      <p class=\"eyebrow\">{escape(SECTIONS[page.section]['name'] if page.section else 'Inicio')}</p>\n"
            f"      <h1>{escape(page.title)}</h1>\n"
            f"      <p class=\"lead\">{escape(page.summary)}</p>\n"
            f"      <p class=\"breadcrumbs\">{breadcrumbs(page)}</p>"
        )
    return (
        "      <div class=\"hero-home-grid\">"
        "        <div>"
        "          <p class=\"eyebrow\">Portal Tecnico</p>"
        "          <h1>Base documental corporativa para una plataforma de trading algoritmico</h1>"
        "          <p class=\"lead\">Documentacion estatica, navegable y preparada para crecer por capas: arquitectura, dominio, modulos, datos, operacion y desarrollo.</p>"
        "          <div class=\"hero-actions\">"
        f"            <a class=\"button\" href=\"{escape(relpath(page, 'arquitectura/index.html'))}\">Explorar arquitectura</a>"
        f"            <a class=\"button button-secondary\" href=\"{escape(relpath(page, 'coverage-index.html'))}\">Ver cobertura documental</a>"
        "          </div>"
        "        </div>"
        "        <div class=\"hero-panel\">"
        "          <p class=\"hero-panel-label\">Resumen ejecutivo</p>"
        "          <ul class=\"hero-metrics\">"
        "            <li><strong>14</strong><span>secciones principales</span></li>"
        "            <li><strong>97</strong><span>paginas HTML iniciales</span></li>"
        "            <li><strong>100%</strong><span>cobertura estructural base</span></li>"
        "          </ul>"
        "          <p class=\"hero-panel-copy\">Todas las areas obligatorias existen desde el inicio y mantienen metadatos visibles para actualizaciones incrementales futuras.</p>"
        "        </div>"
        "      </div>"
        f"      <p class=\"breadcrumbs\">{breadcrumbs(page)}</p>"
    )


def status_panel() -> str:
    return (
        "<aside class=\"status-card\"><h2>Estado de la pagina</h2><dl>"
        "<dt>Estado</dt><dd>Base creada</dd>"
        "<dt>Informacion pendiente</dt><dd><ul>"
        "<li>Completar detalles reales de la aplicacion.</li>"
        "<li>Validar fuentes de verdad y responsables.</li>"
        "</ul></dd>"
        f"<dt>Ultima actualizacion</dt><dd>{escape(TODAY)}</dd></dl></aside>"
    )


def publication_note(page: Page) -> str:
    if page.kind not in {"home", "coverage", "section-index"}:
        return ""
    return (
        "<section class=\"publication-note\">"
        "<div class=\"section-heading\">"
        "<p class=\"section-kicker\">Nota editorial</p>"
        "<h2>Ruta canonica y convencion de publicacion</h2>"
        "</div>"
        "<p>La ruta canonica prevista para publicar esta documentacion es <code>/docs/docs-html/</code>.</p>"
        "<p>El arbol auditado y mantenido en este workspace sigue estando en <code>/docs-html/</code>. Mientras no se alinee la publicacion fisica, esta diferencia debe considerarse un hueco conocido de empaquetado y no de cobertura documental.</p>"
        "<h3>Informacion pendiente</h3>"
        "<ul>"
        "<li>Confirmar cuando la publicacion fisica se mueva o replique a la ruta canonica.</li>"
        "<li>Revisar pipelines, enlaces externos o automatizaciones que dependan de la ruta actual.</li>"
        "</ul>"
        "</section>"
    )


def metadata(page: Page) -> str:
    return (
        f"<!-- doc_id: {page.doc_id} -->\n"
        "<!-- version_doc: 0.1.0 -->\n"
        "<!-- estado: Base creada -->\n"
        f"<!-- ultima_actualizacion: {TODAY} -->\n"
        f"<!-- ruta_canonica_publicacion: {CANONICAL_PUBLICATION_ROUTE} -->\n"
        f"<!-- ruta_workspace_actual: {CURRENT_WORKSPACE_ROUTE} -->\n"
        "<!-- secciones_clave: Proposito, Contenido inicial, Placeholders estructurados -->"
    )


def render(page: Page) -> str:
    mermaid = mermaid_for(page)
    mermaid_html = ""
    if mermaid:
        svg_path = DOCS / mermaid_diagram_relative_path(page)
        svg_src = escape(relpath(page, mermaid_diagram_relative_path(page)))
        image_html = (
            "<figure class=\"diagram-figure\">"
            f"<button class=\"diagram-zoom-trigger\" type=\"button\" data-diagram-src=\"{svg_src}\" data-diagram-alt=\"Version SVG generada para {escape(page.title)}\">"
            f"<img src=\"{svg_src}\" alt=\"Version SVG generada para {escape(page.title)}\" loading=\"lazy\" />"
            "</button>"
            "<figcaption>SVG generado automaticamente a partir del Mermaid de esta pagina. Haz click para ampliarlo sin perdida de nitidez.</figcaption>"
            "</figure>"
        )
        if not svg_path.exists():
            image_html = (
                "<div class=\"diagram-fallback\">"
                "<p class=\"truth-note truth-note-pending\">No se pudo generar el SVG de este diagrama en la ultima regeneracion. "
                "Mantener el Mermaid embebido como fuente de verdad hasta que exista una exportacion valida.</p>"
                "</div>"
            )
        mermaid_html = (
            "<section><h2>Diagrama base</h2>"
            "<p>Esta pagina publica una unica vista SVG generada automaticamente a partir del Mermaid fuente. Haz click sobre la imagen para verla ampliada sin perdida de nitidez.</p>"
            "<div class=\"diagram-stack\">"
            f"{image_html}"
            "</div>"
            "</section>"
        )
    home_visual_html = home_visual(page)
    publication_note_html = publication_note(page)
    return "\n".join(
        [
            "<!DOCTYPE html>",
            "<html lang=\"es\">",
            "<head>",
            "  <meta charset=\"utf-8\" />",
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />",
            f"  <title>{escape(page.title)}</title>",
            f"  <link rel=\"stylesheet\" href=\"{escape(relpath(page, 'assets/styles.css'))}\" />",
            f"  {metadata(page)}",
            "</head>",
            "<body>",
            "  <header class=\"site-header\">",
            f"    <div class=\"brand\"><a href=\"{escape(relpath(page, 'index.html'))}\">Portal Documental</a></div>",
            f"    <nav class=\"top-nav\">{nav(page)}</nav>",
            "  </header>",
            "  <main class=\"layout\">",
            f"    <section class=\"hero{' hero-home' if page.kind == 'home' else ''}\">",
            hero_content(page),
            "    </section>",
            f"    {status_panel()}",
            f"    {publication_note_html}",
            f"    {home_visual_html}",
            f"    {home_cards(page)}",
            f"    {section_page_links(page)}",
            f"    {mermaid_html}",
            f"    {generic_sections(page)}",
            "  </main>",
            "  <div class=\"diagram-lightbox\" hidden aria-hidden=\"true\">",
            "    <div class=\"diagram-lightbox-backdrop\" data-close-lightbox=\"true\"></div>",
            "    <div class=\"diagram-lightbox-dialog\" role=\"dialog\" aria-modal=\"true\" aria-label=\"Vista ampliada del diagrama\">",
            "      <button class=\"diagram-lightbox-close\" type=\"button\" aria-label=\"Cerrar vista ampliada\">Cerrar</button>",
            "      <figure class=\"diagram-lightbox-figure\">",
            "        <img class=\"diagram-lightbox-image\" src=\"\" alt=\"\" />",
            "        <figcaption class=\"diagram-lightbox-caption\">Vista ampliada del diagrama generado.</figcaption>",
            "      </figure>",
            "    </div>",
            "  </div>",
            "  <footer class=\"site-footer\">",
            "    <p>Base documental HTML estatica preparada para actualizaciones incrementales.</p>",
            f"    <p>Ultima generacion: {escape(TODAY)}</p>",
            "  </footer>",
            f"  <script src=\"{escape(relpath(page, 'assets/app.js'))}\"></script>",
            "</body>",
            "</html>",
        ]
    )


def styles() -> str:
    return """
:root {
  --bg: #edf1f5;
  --surface: #ffffff;
  --surface-2: #e4ebf2;
  --surface-3: #f6f8fb;
  --text: #182230;
  --muted: #5d6877;
  --line: #cfd7e3;
  --accent: #0d4a74;
  --accent-2: #0a2f4a;
  --accent-3: #d29a2e;
  --warn: #8d5c00;
  --max: 1240px;
  --radius: 20px;
  --shadow: 0 18px 50px rgba(17, 33, 52, 0.08);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: "Segoe UI", Arial, sans-serif;
  color: var(--text);
  background:
    radial-gradient(circle at top left, rgba(13, 74, 116, 0.12), transparent 22%),
    linear-gradient(180deg, #f7f9fc 0%, var(--bg) 100%);
}
a { color: var(--accent-2); text-decoration: none; }
a:hover { text-decoration: underline; }
.site-header {
  position: sticky;
  top: 0;
  z-index: 10;
  display: flex;
  justify-content: space-between;
  gap: 1rem;
  padding: 1rem 2rem;
  background: rgba(247, 249, 252, 0.92);
  border-bottom: 1px solid rgba(207, 215, 227, 0.9);
  backdrop-filter: blur(10px);
}
.brand a {
  font-weight: 700;
  color: var(--accent-2);
  letter-spacing: 0.04em;
  text-transform: uppercase;
  font-size: 0.92rem;
}
.top-nav { display: flex; flex-wrap: wrap; gap: 0.5rem; }
.nav-link {
  padding: 0.55rem 0.9rem;
  border-radius: 999px;
  color: var(--muted);
  border: 1px solid transparent;
}
.nav-link.active, .nav-link:hover {
  background: rgba(13, 74, 116, 0.08);
  color: var(--accent-2);
  border-color: rgba(13, 74, 116, 0.12);
  text-decoration: none;
}
.layout {
  width: min(calc(100% - 2rem), var(--max));
  margin: 0 auto;
  padding: 2rem 0 4rem;
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  gap: 1.5rem;
}
.hero, .status-card, .card, section {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 1.4rem 1.6rem;
}
.hero {
  grid-column: 1 / -1;
  padding: 2rem;
  background:
    linear-gradient(135deg, rgba(10, 47, 74, 0.98), rgba(13, 74, 116, 0.92)),
    linear-gradient(180deg, #18344f 0%, #0f2b44 100%);
  color: #f8fbff;
  border-color: rgba(13, 74, 116, 0.28);
}
.hero-home {
  position: relative;
  overflow: hidden;
}
.hero-home::after {
  content: "";
  position: absolute;
  inset: auto -5% -30% auto;
  width: 360px;
  height: 360px;
  background: radial-gradient(circle, rgba(210, 154, 46, 0.24), transparent 62%);
  pointer-events: none;
}
.hero h1 {
  margin: 0.2rem 0 0.7rem;
  font-size: clamp(2.2rem, 4.5vw, 4rem);
  line-height: 1.02;
  max-width: 12ch;
  font-family: Georgia, "Times New Roman", serif;
}
.eyebrow {
  margin: 0;
  color: rgba(248, 251, 255, 0.78);
  font-size: 0.84rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-weight: 700;
}
.lead {
  color: rgba(248, 251, 255, 0.84);
  max-width: 62ch;
  font-size: 1.08rem;
}
.breadcrumbs {
  color: rgba(248, 251, 255, 0.72);
  font-size: 0.93rem;
}
.breadcrumbs a { color: rgba(248, 251, 255, 0.88); }
.hero-home-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(280px, 0.8fr);
  gap: 1.4rem;
  align-items: end;
}
.hero-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.8rem;
  margin-top: 1.3rem;
}
.hero-panel {
  background: rgba(255, 255, 255, 0.09);
  border: 1px solid rgba(255, 255, 255, 0.12);
  border-radius: calc(var(--radius) - 4px);
  padding: 1.2rem 1.25rem;
  backdrop-filter: blur(10px);
}
.hero-panel-label {
  margin: 0 0 0.9rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.76rem;
  color: rgba(248, 251, 255, 0.72);
}
.hero-metrics {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.8rem;
}
.hero-metrics li {
  display: grid;
  gap: 0.2rem;
}
.hero-metrics strong {
  font-size: 1.6rem;
  color: #ffffff;
}
.hero-metrics span {
  color: rgba(248, 251, 255, 0.72);
  font-size: 0.82rem;
}
.hero-panel-copy {
  margin: 1rem 0 0;
  color: rgba(248, 251, 255, 0.8);
}
.status-card { position: sticky; top: 5.5rem; align-self: start; }
.status-card dt { font-weight: 700; margin-top: 1rem; }
.status-card dd { margin-left: 0; color: var(--muted); }
.layout > section:not(.hero), .card-grid { grid-column: 1 / 2; }
.card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; }
.card {
  background:
    linear-gradient(180deg, rgba(246, 248, 251, 0.98), rgba(255, 255, 255, 1)),
    linear-gradient(180deg, rgba(13, 74, 116, 0.04), transparent);
}
.card h3 {
  margin-top: 0;
  font-family: Georgia, "Times New Roman", serif;
}
.button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.7rem 1rem;
  border-radius: 999px;
  background: var(--accent-3);
  color: var(--accent-2);
  font-weight: 700;
  border: 1px solid rgba(210, 154, 46, 0.35);
}
.button-secondary {
  background: transparent;
  color: #f8fbff;
  border-color: rgba(248, 251, 255, 0.22);
}
.button:hover {
  background: #e4ad3f;
  text-decoration: none;
}
.button-secondary:hover {
  background: rgba(255, 255, 255, 0.08);
  color: white;
  text-decoration: none;
}
.link-list { list-style: none; padding-left: 0; margin: 0; }
.link-list li {
  display: flex;
  justify-content: space-between;
  gap: 1rem;
  align-items: center;
  padding: 0.8rem 0;
  border-bottom: 1px solid var(--line);
}
.related-links {
  list-style: none;
  padding: 0;
  margin: 1rem 0 0;
  display: grid;
  gap: 0.9rem;
}
.related-links li {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--surface-3);
  padding: 0.95rem 1rem;
}
.related-link-body {
  display: grid;
  gap: 0.35rem;
}
.related-link-meta {
  margin: 0;
  color: var(--accent);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.74rem;
  font-weight: 700;
}
.related-link-title {
  font-weight: 700;
  color: var(--accent-2);
}
.related-link-copy {
  margin: 0;
  color: var(--muted);
  font-size: 0.94rem;
}
.truth-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
  gap: 1rem;
  margin-top: 1rem;
}
.truth-card {
  background: var(--surface-3);
  border: 1px solid var(--line);
  border-radius: 16px;
  padding: 1rem;
}
.truth-card h3 {
  margin: 0 0 0.8rem;
  font-size: 1rem;
}
.truth-list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  gap: 0.8rem;
}
.truth-list li {
  padding-top: 0.1rem;
}
.truth-link,
.truth-path-label {
  font-weight: 700;
  color: var(--accent-2);
  margin: 0;
}
.truth-path {
  display: inline-block;
  margin-top: 0.25rem;
  padding: 0.2rem 0.45rem;
  border-radius: 8px;
  background: var(--surface-2);
  border: 1px solid var(--line);
  color: var(--accent-2);
  font-family: Consolas, monospace;
}
.truth-note {
  margin: 0.35rem 0 0;
  color: var(--muted);
  font-size: 0.92rem;
}
.truth-note-pending {
  color: var(--warn);
}
.status-pill {
  padding: 0.2rem 0.55rem;
  border-radius: 999px;
  background: rgba(13, 74, 116, 0.08);
  color: var(--accent-2);
  font-size: 0.8rem;
}
table { width: 100%; border-collapse: collapse; background: var(--surface); }
th, td { text-align: left; padding: 0.85rem 0.9rem; border-bottom: 1px solid var(--line); vertical-align: top; }
th { background: var(--surface-2); }
.faq-list dt { font-weight: 700; margin-top: 1rem; }
.faq-list dd { margin: 0.25rem 0 0; }
.placeholder { color: var(--warn); }
.section-heading { margin-bottom: 1rem; }
.section-kicker {
  margin: 0 0 0.4rem;
  color: var(--accent);
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.78rem;
  font-weight: 700;
}
.diagram-figure {
  margin: 0;
  display: grid;
  gap: 0.8rem;
}
.diagram-stack {
  display: grid;
  gap: 1rem;
}
.diagram-fallback {
  border: 1px solid var(--line);
  border-radius: 16px;
  background: var(--surface-3);
  padding: 1rem;
}
.diagram-zoom-trigger {
  display: block;
  width: 100%;
  padding: 0;
  border: 0;
  background: transparent;
  cursor: zoom-in;
}
.diagram-figure img {
  width: 100%;
  display: block;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: #ffffff;
  box-shadow: 0 22px 50px rgba(17, 33, 52, 0.12);
}
.diagram-figure figcaption {
  color: var(--muted);
  font-size: 0.92rem;
}
.diagram-lightbox[hidden] {
  display: none;
}
.diagram-lightbox {
  position: fixed;
  inset: 0;
  z-index: 1000;
}
.diagram-lightbox-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(9, 18, 28, 0.82);
  backdrop-filter: blur(8px);
}
.diagram-lightbox-dialog {
  position: relative;
  z-index: 1;
  width: min(calc(100% - 2rem), 1500px);
  max-height: calc(100vh - 2rem);
  margin: 1rem auto;
  padding: 1rem;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.96);
  border: 1px solid rgba(207, 215, 227, 0.8);
  box-shadow: 0 28px 60px rgba(0, 0, 0, 0.28);
  overflow: auto;
}
.diagram-lightbox-close {
  display: inline-flex;
  margin-left: auto;
  padding: 0.6rem 0.9rem;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: var(--surface-2);
  color: var(--accent-2);
  font-weight: 700;
  cursor: pointer;
}
.diagram-lightbox-figure {
  margin: 0.8rem 0 0;
  display: grid;
  gap: 0.8rem;
}
.diagram-lightbox-image {
  display: block;
  width: auto;
  max-width: 100%;
  height: auto;
  margin: 0 auto;
  image-rendering: -webkit-optimize-contrast;
  image-rendering: crisp-edges;
}
.diagram-lightbox-caption {
  text-align: center;
  color: var(--muted);
  font-size: 0.92rem;
}
.publication-note code {
  padding: 0.15rem 0.4rem;
  border-radius: 8px;
  background: var(--surface-2);
  border: 1px solid var(--line);
  color: var(--accent-2);
  font-family: Consolas, monospace;
}
.publication-note h3 {
  margin-bottom: 0.5rem;
}
.site-footer {
  width: min(calc(100% - 2rem), var(--max));
  margin: 0 auto 2rem;
  color: var(--muted);
}
.mermaid { overflow-x: auto; }
@media (max-width: 960px) {
  .layout { grid-template-columns: 1fr; }
  .status-card { position: static; }
  .hero-home-grid { grid-template-columns: 1fr; }
  .hero-metrics { grid-template-columns: repeat(3, minmax(0, 1fr)); }
}
@media (max-width: 720px) {
  .site-header { position: static; padding: 1rem; }
  .layout { width: min(calc(100% - 1rem), var(--max)); padding-top: 1rem; }
  .hero { padding: 1.4rem; }
  .hero h1 { max-width: none; }
  .hero-metrics { grid-template-columns: 1fr; }
  .hero-actions { flex-direction: column; align-items: stretch; }
  .diagram-lightbox-dialog { width: min(calc(100% - 1rem), 1500px); margin: 0.5rem auto; }
}
"""


def app_js() -> str:
    return """
const lightbox = document.querySelector(".diagram-lightbox");
const lightboxImage = lightbox?.querySelector(".diagram-lightbox-image");
const lightboxCaption = lightbox?.querySelector(".diagram-lightbox-caption");
const lightboxClose = lightbox?.querySelector(".diagram-lightbox-close");

function openDiagramLightbox(trigger) {
  if (!lightbox || !lightboxImage || !lightboxCaption) return;
  const src = trigger.getAttribute("data-diagram-src");
  const alt = trigger.getAttribute("data-diagram-alt") || "Vista ampliada del diagrama";
  if (!src) return;
  lightboxImage.src = src;
  lightboxImage.alt = alt;
  lightboxCaption.textContent = alt;
  lightbox.hidden = false;
  lightbox.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";
}

function closeDiagramLightbox() {
  if (!lightbox || !lightboxImage) return;
  lightbox.hidden = true;
  lightbox.setAttribute("aria-hidden", "true");
  lightboxImage.src = "";
  document.body.style.overflow = "";
}

document.querySelectorAll(".diagram-zoom-trigger").forEach((trigger) => {
  trigger.addEventListener("click", () => openDiagramLightbox(trigger));
});

lightboxClose?.addEventListener("click", closeDiagramLightbox);
lightbox?.addEventListener("click", (event) => {
  const target = event.target;
  if (target instanceof HTMLElement && target.hasAttribute("data-close-lightbox")) {
    closeDiagramLightbox();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && lightbox && !lightbox.hidden) {
    closeDiagramLightbox();
  }
});
"""


def build() -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    for section in SECTIONS:
        if section:
            (DOCS / section).mkdir(parents=True, exist_ok=True)
    (DOCS / "assets").mkdir(parents=True, exist_ok=True)
    diagrams_dir = mermaid_diagrams_dir()
    diagrams_dir.mkdir(parents=True, exist_ok=True)
    for diagram_path in diagrams_dir.iterdir():
        if diagram_path.is_file():
            diagram_path.unlink()
    svg_cache: dict[str, str | None] = {}
    for page in PAGES:
        mermaid = mermaid_for(page)
        if not mermaid:
            continue
        rendered_svg = svg_cache.get(mermaid)
        if mermaid not in svg_cache:
            rendered_svg = render_mermaid_svg(mermaid)
            svg_cache[mermaid] = rendered_svg
        if rendered_svg:
            (diagrams_dir / mermaid_diagram_filename(page)).write_text(rendered_svg, encoding="utf-8")
    for page in PAGES:
        page.path.parent.mkdir(parents=True, exist_ok=True)
        page.path.write_text(render(page), encoding="utf-8")
    (DOCS / "assets" / "styles.css").write_text(styles(), encoding="utf-8")
    (DOCS / "assets" / "app.js").write_text(app_js(), encoding="utf-8")
    if HOME_DIAGRAM_SOURCE.exists():
        shutil.copy2(HOME_DIAGRAM_SOURCE, DOCS / "assets" / HOME_DIAGRAM_NAME)


if __name__ == "__main__":
    build()
    print(f"Generadas {len(PAGES)} paginas en {DOCS}")
