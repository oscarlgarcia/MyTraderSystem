from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from html import escape, unescape
import json
from pathlib import Path
import re
import subprocess
import shutil
import tempfile


GENERATED_AT = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M")
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
        ("WebUI", "Interfaz web interna del plano de control que consume un read model desacoplado y solo encola comandos operativos."),
        ("Read Model", "Vista materializada para consultas de UI construida desde telemetria append-only y no desde el hot path vivo."),
        ("Command Gateway", "Superficie API que recibe acciones de operador y las convierte en comandos asincronos auditables."),
        ("Ownership Tecnico Observable", "Paquete, modulo o superficie de codigo que concentra una capacidad cuando no existe un owner humano verificado en la documentacion."),
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
        ("Hot Path", "Camino de procesamiento critico del runtime que no debe compartir consultas ni locks con la UI operativa."),
        ("Telemetry Path", "Camino append-only y best-effort donde el runtime emite estado operativo desacoplado del hot path."),
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
SEARCH_PAGE = Page("", "search", "Buscar en la Documentacion", "Buscador navegable del portal documental.", "search")

SEARCH_CARD_SECTIONS = (
    "arquitectura",
    "modulos",
    "flujos",
    "datos",
    "research-simulation",
    "operacion",
    "desarrollo",
    "seguridad-gobernanza",
    "glosario",
)


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
    "arquitectura/capability-map": [
        ("modulos/index.html", "Conecta cada capacidad con el modulo documental principal que la implementa."),
        ("modulos/ingestion.html", "Profundiza la capacidad de market data y persistencia observada en el repo."),
        ("modulos/feature-store-feature-engine.html", "Aterriza la capacidad de feature engineering y estado cuantitativo."),
        ("operacion/service-catalog.html", "Relaciona ownership tecnico observable con responsabilidad operativa."),
    ],
    "arquitectura/deployment": [
        ("operacion/deployment-release.html", "Conecta la vista de despliegue con el procedimiento operativo de build, up y test."),
        ("desarrollo/environment-configuration.html", "Relaciona entornos y archivos de configuracion con el despliegue visible."),
        ("arquitectura/topologia-runtime.html", "Baja del empaquetado y entorno a los procesos y relaciones runtime."),
        ("operacion/service-catalog.html", "Aterriza la unidad desplegable visible en el servicio operativo principal."),
    ],
    "arquitectura/topologia-runtime": [
        ("arquitectura/deployment.html", "Conecta la topologia en ejecucion con la unidad desplegable y el entorno que la aloja."),
        ("flujos/startup-recovery-sequence.html", "Relaciona los procesos runtime con el arranque y la recuperacion."),
        ("modulos/index.html", "Permite bajar desde la topologia a cada modulo participante del ciclo."),
        ("operacion/troubleshooting.html", "Conecta procesos y storage con diagnostico operativo."),
    ],
    "arquitectura/dependencias-servicios": [
        ("arquitectura/topologia-runtime.html", "Relaciona dependencias tecnicas con procesos y superficies runtime verificadas."),
        ("arquitectura/deployment.html", "Conecta dependencias externas e internas con la unidad desplegable visible."),
        ("operacion/service-catalog.html", "Aterriza criticidad e impacto por fallo en una vista operativa."),
        ("operacion/troubleshooting.html", "Permite enlazar dependencias criticas con diagnostico y mitigacion."),
    ],
    "arquitectura/observabilidad": [
        ("operacion/slo-sla-alerting.html", "Conecta las señales tecnicas con objetivos y alertas operativas."),
        ("operacion/troubleshooting.html", "Relaciona logs y metricas con diagnostico y respuesta."),
        ("arquitectura/health-model.html", "Completa la observabilidad con estados de salud y degradacion."),
        ("operacion/incident-playbooks.html", "Permite enlazar deteccion con respuesta ante incidentes."),
    ],
    "arquitectura/health-model": [
        ("arquitectura/observabilidad.html", "Conecta estados de salud con las señales tecnicas que los sustentan."),
        ("operacion/slo-sla-alerting.html", "Relaciona degradacion y criticidad con umbrales operativos."),
        ("operacion/service-catalog.html", "Aterriza el modelo de salud sobre servicios y capacidades operativas."),
        ("operacion/troubleshooting.html", "Permite bajar del estado de salud al diagnostico concreto."),
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
    "modulos/control-plane-ops": [
        ("arquitectura/deployment.html", "Conecta la UI y el worker con las unidades desplegables reales de Docker Compose."),
        ("arquitectura/topologia-runtime.html", "Relaciona el control plane con la topologia runtime y el desacople respecto al hot path."),
        ("operacion/service-catalog.html", "Aterriza webUI y control-plane-worker como servicios operativos concretos."),
        ("arquitectura/observabilidad.html", "Conecta telemetria, alertas y read model con las senales operativas existentes."),
        ("operacion/troubleshooting.html", "Permite enlazar las superficies de control plane con diagnostico y recovery."),
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
        ("modulos/control-plane-ops.html", "Profundiza el modulo que implementa la UI web interna, el worker y el read model."),
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
            path_ref("Codigo del control plane", "app/controlplane/", "Fuente de verdad principal del webUI, read model, stores y worker."),
            path_ref("Ops y observabilidad reutilizados", "app/ops/, app/observability/", "Superficies operativas y de alertado reutilizadas por el control plane."),
            path_ref("Despliegue y arranque local", "docker-compose.yml, Makefile", "Servicios webUI y control-plane-worker verificados en Compose."),
            path_ref("Pruebas del control plane", "tests/controlplane/, tests/ingestion/test_controlplane_telemetry.py", "Cobertura automatizada verificada para API, builder, store, worker y telemetria."),
        ],
        "Referencias operativas": [
            doc_ref("Catalogo de Servicios", "operacion/service-catalog.html", "Aterriza webUI y worker como servicios operativos."),
            doc_ref("Topologia Runtime", "arquitectura/topologia-runtime.html", "Relaciona el modulo con procesos, contenedores y filesystem compartido."),
        ],
        "Artefactos vivos": [
            path_ref("Telemetria desacoplada", "data/<env>/ui-telemetry/", "Destino real de JSONL append-only consumidos por el read model."),
            path_ref("Read model SQLite", "data/<env>/control-plane/control_plane.sqlite", "Base de datos separada de control plane usada por la UI y el worker."),
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
        "Repositorio y codigo": [
            path_ref("Servicios y contenedores", "docker-compose.yml", "Fuente de verdad del inventario Compose visible en esta iteracion."),
            path_ref("Control plane", "app/controlplane/", "Implementacion de webUI, read model y worker."),
            path_ref("Pruebas operativas del control plane", "tests/controlplane/", "Cobertura verificada para store, builder, API y worker."),
        ],
        "Referencias operativas": [
            path_ref("Runbooks de ingestion", "docs/operations/ingestion_runbook.md, docs/operations/ingestion_promotion_runbook.md, docs/operations/ingestion_rollback_checklist.md", "Servicios con cobertura operativa verificada en el repo."),
            path_ref("Live cutover", "docs/ops/live_cutover.md", "Referencia operativa adicional."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias operativas", "docs/validation/", "Artefactos vivos presentes en el repo."),
            path_ref("Telemetria y read model", "data/<env>/ui-telemetry/, data/<env>/control-plane/", "Artefactos operativos usados por la UI de ingestion v1."),
        ],
    },
    "arquitectura/deployment": {
        "Repositorio y codigo": [
            path_ref("Definicion de despliegue local", "docker-compose.yml, Dockerfile", "Fuente de verdad de servicios, imagenes y puertos visibles."),
            path_ref("Codigo del control plane", "app/controlplane/", "Implementacion de webUI y worker desplegados aparte del engine."),
            path_ref("Scripts y helpers locales", "Makefile, scripts/docker-*.ps1", "Entrypoints operativos locales visibles en el repo."),
            path_ref("Configuracion", "app/config.py, config.dev.yaml, config.test.yaml, config.prod.yaml", "Claves de entorno y rutas del control plane y del runtime principal."),
        ],
        "Referencias operativas": [
            doc_ref("Catalogo de Servicios", "operacion/service-catalog.html", "Aterriza los servicios y superficies operativas resultantes."),
            doc_ref("Modulo Plano de Control / Operaciones", "modulos/control-plane-ops.html", "Describe el modulo desplegado en webUI y control-plane-worker."),
        ],
    },
    "arquitectura/topologia-runtime": {
        "Repositorio y codigo": [
            path_ref("Entrypoints runtime", "app/main.py, app/controlplane/api.py, app/controlplane/worker.py", "Puntos de entrada verificados del engine, la UI y el worker."),
            path_ref("Builder y stores", "app/controlplane/builder.py, app/controlplane/store.py, app/controlplane/sqlite_store.py", "Implementacion del read model y del store desacoplado."),
            path_ref("Plantillas UI", "app/controlplane/templates/", "Pantallas HTML reales de la UI v1."),
            path_ref("Topologia de contenedores", "docker-compose.yml", "Relaciones runtime verificadas entre app, webUI y control-plane-worker."),
        ],
        "Referencias operativas": [
            doc_ref("Catalogo de Servicios", "operacion/service-catalog.html", "Relaciona nodos runtime con ownership y operaciones."),
            doc_ref("Modulo Plano de Control / Operaciones", "modulos/control-plane-ops.html", "Profundiza la logica del control plane y sus pantallas."),
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
    "arquitectura/dependencias-servicios": {
        "Repositorio y codigo": [
            path_ref("Inventario de dependencias", "docs/dependencies.md", "Fuente de verdad principal para modulos, acoplamientos y riesgos ya identificados."),
            path_ref("Dependencias Python", "pyproject.toml, poetry.lock", "Librerias externas verificadas para runtime y desarrollo."),
            path_ref("Entry point y wiring", "app/main.py", "Muestra el cableado principal entre ingestion, features, risk, execution y portfolio."),
        ],
        "Referencias operativas": [
            doc_ref("Catalogo de Servicios", "operacion/service-catalog.html", "Complementa criticidad tecnica con lectura operativa."),
            doc_ref("Resolucion de Problemas", "operacion/troubleshooting.html", "Conecta dependencias criticas con diagnostico y mitigacion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Artefactos vivos disponibles para validar cambios sobre dependencias criticas."),
        ],
    },
    "arquitectura/observabilidad": {
        "Repositorio y codigo": [
            path_ref("Logger estructurado", "app/observability/logger.py", "Define el formato JSON, trace_id y redaccion de campos sensibles."),
            path_ref("Catalogo de alertas", "app/observability/alerts.py", "Define tipos de alerta, severidad, umbrales y acciones recomendadas."),
            path_ref("Contrato de observabilidad", "app/ops/observability_contract.py", "Lista metricas y alertas requeridas con thresholds paper/live."),
        ],
        "Referencias operativas": [
            path_ref("Runbook de ingestion", "docs/operations/ingestion_runbook.md", "Enumera señales operativas a revisar y criterio GO/NO-GO."),
            doc_ref("SLO / SLA / Alertas", "operacion/slo-sla-alerting.html", "Relaciona señales tecnicas con umbrales operativos."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Reportes de readiness, canary, soak y release gates consumidos operativamente."),
        ],
    },
    "arquitectura/health-model": {
        "Repositorio y codigo": [
            path_ref("Readiness orchestrator", "app/ops/readiness_orchestrator.py", "Genera reportes PASS/FAIL a partir de checks concretos."),
            path_ref("Release gates", "app/ops/release_gates.py", "Consolida bloques de estado para support matrix, exact recovery, canaries y storage health."),
            path_ref("Storage health", "app/ingestion/storage_health.py", "Calcula backlog, lag de compactacion y fallos de storage."),
        ],
        "Referencias operativas": [
            path_ref("Runbook de ingestion", "docs/operations/ingestion_runbook.md", "Usa ingestion health y storage health como señales operativas."),
            doc_ref("Resolucion de Problemas", "operacion/troubleshooting.html", "Conecta estados FAIL o degradados con mitigacion."),
        ],
        "Artefactos vivos": [
            path_ref("Evidencias de validacion", "docs/validation/", "Readiness reports, canaries y gate reports usados como evidencia de salud."),
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


NAV_DIRECT_LINKS = [
    ("", "Inicio", "home"),
    ("modulos", "Modulos", "modules"),
]

NAV_GROUPS = [
    ("Portal y Contexto", "portal", [("arquitectura", "Arquitectura"), ("dominio-trading", "Dominio Trading")]),
    ("Flujos y Datos", "flow", [("flujos", "Flujos y Secuencias"), ("datos", "Datos")]),
    (
        "Research y Operacion",
        "research",
        [("research-simulation", "Investigacion / Backtesting / Simulacion"), ("operacion", "Operacion")],
    ),
    (
        "Desarrollo, Gobierno y Soporte",
        "support",
        [
            ("desarrollo", "Desarrollo"),
            ("seguridad-gobernanza", "Seguridad y Gobernanza"),
            ("decisiones", "Decisiones"),
            ("glosario", "Glosario"),
            ("faq", "Preguntas Frecuentes"),
            ("postmortems", "Postmortems"),
            ("templates", "Plantillas"),
        ],
    ),
]

SECTION_ICON_MAP = {
    "": "home",
    "arquitectura": "architecture",
    "dominio-trading": "portal",
    "modulos": "modules",
    "flujos": "flow",
    "datos": "data",
    "research-simulation": "research",
    "operacion": "operations",
    "desarrollo": "development",
    "seguridad-gobernanza": "security",
    "decisiones": "decision",
    "glosario": "glossary",
    "faq": "faq",
    "postmortems": "postmortem",
    "templates": "template",
}

SEARCH_SUGGESTIONS = (
    "ingestion",
    "observabilidad",
    "features",
    "runbooks",
)


def nav(current: Page) -> str:
    items = []
    for section, label, icon_name in NAV_DIRECT_LINKS:
        target = "index.html" if not section else f"{section}/index.html"
        active = " active" if current.url == target else ""
        items.append(
            f'<a class="nav-link-direct{active}" href="{escape(relpath(current, target))}">'
            f'{icon(current, icon_name)}<span>{escape(label)}</span></a>'
        )
    for group_label, icon_name, links in NAV_GROUPS:
        targets = [f"{section}/index.html" for section, _label in links]
        is_active_group = current.url in targets or any(section == current.section for section, _label in links)
        active_group_class = " active" if is_active_group else ""
        open_attr = " open" if is_active_group else ""
        sublinks = []
        for section, label in links:
            target = f"{section}/index.html"
            active_link_class = " active" if current.url == target else ""
            sublinks.append(
                f'<a class="nav-sublink{active_link_class}" href="{escape(relpath(current, target))}">{escape(label)}</a>'
            )
        items.append(
            f'<details class="nav-group{active_group_class}"{open_attr}>'
            f'<summary class="nav-group-summary">{icon(current, icon_name)}<span>{escape(group_label)}</span></summary>'
            f'<div class="nav-group-panel">{"".join(sublinks)}</div>'
            "</details>"
        )
    return "".join(items)


def icon(current: Page, name: str, *, cls: str = "ui-icon", label: str | None = None) -> str:
    href = escape(relpath(current, f"assets/icons.svg#{name}"))
    aria = f' aria-hidden="true"' if label is None else f' role="img" aria-label="{escape(label)}"'
    return f'<svg class="{cls}"{aria}><use href="{href}"></use></svg>'


def search_form(current: Page) -> str:
    action = escape(relpath(current, SEARCH_PAGE.url))
    control_id = escape(current.doc_id.replace("/", "-"))
    return (
        f'<form class="site-search-form" action="{action}" method="get" role="search">'
        f'<label class="sr-only" for="site-search-q-{control_id}">Buscar en la documentacion</label>'
        f'<div class="site-search-shell">{icon(current, "search", cls="ui-icon ui-icon-search")}'
        f'<input id="site-search-q-{control_id}" class="site-search-input" type="search" name="q" placeholder="Buscar en la documentacion" autocomplete="off" />'
        f'<button class="site-search-submit" type="submit">{icon(current, "arrow-right", cls="ui-icon ui-icon-submit", label="Buscar")}</button>'
        "</div></form>"
    )


def header_html(current: Page) -> str:
    return (
        '<header class="site-header">'
        f'<div class="brand"><a href="{escape(relpath(current, "index.html"))}">Portal Documental</a></div>'
        f'<nav class="top-nav" aria-label="Navegacion principal">{nav(current)}</nav>'
        f"{search_form(current)}"
        "</header>"
    )


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


def strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def page_search_source_html(page: Page) -> str:
    chunks = [generic_sections(page)]
    if page.kind == "home":
        chunks.append(home_cards(page))
    elif page.kind == "section-index":
        chunks.append(section_page_links(page))
    return "\n".join(chunk for chunk in chunks if chunk)


def search_document(page: Page) -> dict[str, object]:
    source_html = page_search_source_html(page)
    headings = [
        heading
        for heading in (
            strip_html(match.group(1))
            for match in re.finditer(r"<h[23][^>]*>(.*?)</h[23]>", source_html, flags=re.IGNORECASE | re.DOTALL)
        )
        if heading
    ]
    paragraphs = [
        paragraph
        for paragraph in (
            strip_html(match.group(1))
            for match in re.finditer(r"<p[^>]*>(.*?)</p>", source_html, flags=re.IGNORECASE | re.DOTALL)
        )
        if paragraph
    ]
    excerpt = next((paragraph for paragraph in paragraphs if len(paragraph) >= 60), paragraphs[0] if paragraphs else page.summary)
    body = strip_html(source_html)
    return {
        "id": page.doc_id,
        "url": page.url,
        "title": page.title,
        "section": page.section,
        "section_name": SECTIONS[page.section]["name"] if page.section else "Inicio",
        "summary": page.summary,
        "headings": headings,
        "excerpt": excerpt[:260],
        "body": body,
        "updated_at": GENERATED_AT,
    }


def search_index_payload() -> list[dict[str, object]]:
    return [search_document(page) for page in PAGES]


def mermaid_diagrams_dir() -> Path:
    return DOCS / "assets" / MERMAID_DIAGRAMS_DIRNAME


def mermaid_diagram_filename(page: Page) -> str:
    section = (page.section or "portal").replace("/", "-")
    slug = page.slug.replace("/", "-")
    return f"{section}--{slug}.svg"


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
      flowchart: {{ curve: "basis", htmlLabels: false }}
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
        svg = match.group(1)
        # Mermaid puede emitir <br> dentro de foreignObject; como el SVG se
        # sirve como XML standalone, esos void tags deben autocerrarse.
        svg = svg.replace("<br>", "<br />")
        return svg


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
    if page.doc_id == "arquitectura/capability-map":
        return """
flowchart TB
  Cap["Mapa de capacidades"] --> Cfg["Configuracion y arranque"]
  Cap --> Md["Market data e ingestion"]
  Cap --> Feat["Feature engineering"]
  Cap --> Dec["Decision cuantitativa"]
  Cap --> Exec["Ejecucion y portfolio"]
  Cap --> Ops["Ops y observabilidad"]

  Cfg --> M1["app.main / app.config"]
  Md --> M2["app.ingestion / app.marketdata"]
  Feat --> M3["app.features"]
  Dec --> M4["app.strategy / app.risk"]
  Exec --> M5["app.execution / app.portfolio"]
  Ops --> M6["app.ops / app.observability"]
"""
    if page.doc_id == "arquitectura/deployment":
        return """
flowchart TB
  Host["Host Docker local"] --> App["Servicio app"]
  Host --> WebUI["Servicio webUI :8001"]
  Host --> Worker["Servicio control-plane-worker"]
  App --> Workspace["Bind mount /workspace"]
  WebUI --> Workspace
  Worker --> Workspace
  App --> Telemetry["data/<env>/ui-telemetry"]
  WebUI --> Telemetry
  Worker --> Telemetry
  WebUI --> CPDB["data/<env>/control-plane/control_plane.sqlite"]
  Worker --> CPDB
  Browser["Browser operador"] --> WebUI
  App --> Binance["Binance REST y WS"]
"""
    if page.doc_id == "arquitectura/topologia-runtime":
        return """
flowchart LR
  Browser["Browser operador"] --> WebUI["webUI / FastAPI"]
  WebUI --> Builder["ReadModelBuilder.sync_once"]
  Builder --> Telemetry["Telemetry JSONL"]
  Builder --> CPDB["control_plane.sqlite"]
  WebUI --> CPDB
  Worker["control-plane-worker"] --> CPDB
  Worker --> Ops["ack_alert / resync_stream / replay_range"]
  CLI["CLI / app.main"] --> Ingestion["run_ingestion_service"]
  Ingestion --> Features["FeatureEngine / pipeline"]
  Features --> Strategy["strategy"]
  Strategy --> Risk["risk"]
  Risk --> Execution["execution.paper"]
  Execution --> Portfolio["portfolio.state"]
  Ingestion --> Storage["Parquet / checkpoints / metadata"]
  Ingestion --> Telemetry
  Ops --> Telemetry
"""
    if page.doc_id == "modulos/control-plane-ops":
        return """
flowchart LR
  Operator["Operador tecnico"] --> WebUI["webUI / FastAPI + Jinja2 + HTMX"]
  WebUI --> Builder["ReadModelBuilder.sync_once"]
  Builder --> Telemetry["ui-telemetry JSONL"]
  Builder --> DB["control_plane.sqlite"]
  WebUI --> DB
  WebUI --> Queue["command_requests"]
  Worker["control-plane-worker"] --> Queue
  Worker --> Ops["ack_alert / resync_stream / replay_range"]
  Ops --> Audit["command_audit + recovery_command_audit"]
  Audit --> DB
"""
    if page.doc_id == "operacion/service-catalog":
        return """
flowchart LR
  Browser["Browser operador"] --> WebUI["Servicio webUI"]
  App["Servicio app"] --> Telemetry["ui-telemetry JSONL"]
  WebUI --> Builder["sync_once"]
  Builder["Read model builder"] --> Telemetry
  Builder --> DB["control_plane.sqlite"]
  Worker["Servicio control-plane-worker"] --> DB
  Worker --> Ops["Recovery commands"]
  Ops --> Telemetry
"""
    if page.doc_id == "arquitectura/dependencias-servicios":
        return """
flowchart LR
  CLI["app.main / app.config"] --> Ingestion["app.ingestion"]
  CLI --> Features["app.features"]
  CLI --> Strategy["app.strategy"]
  CLI --> Risk["app.risk"]
  CLI --> Execution["app.execution"]
  CLI --> Portfolio["app.portfolio"]
  CLI --> Obs["app.observability / app.ops"]
  Ingestion --> Binance["Binance REST y WS"]
  Ingestion --> Storage["Filesystem local + pyarrow"]
  Features --> Storage
  Execution --> Portfolio
"""
    if page.doc_id == "arquitectura/observabilidad":
        return """
flowchart LR
  Runtime["app.main / runtime"] --> Logs["logger JSON + trace_id"]
  Runtime --> Metrics["ingestion summary / ingestion health"]
  Runtime --> Alerts["operational alert"]
  Metrics --> Gates["release gates / readiness"]
  Alerts --> Gates
  Storage["storage health"] --> Metrics
  Storage --> Alerts
"""
    if page.doc_id == "arquitectura/health-model":
        return """
flowchart TB
  Healthy["SALUDABLE"] -->|heartbeat, storage y canaries ok| Ready["READY"]
  Ready -->|gaps, skew o backlog alto| Degraded["DEGRADADO"]
  Degraded -->|gap irreparable, sink failure, compaction failure o gates FAIL| Failed["NO READY / FAIL"]
  Failed --> Recover["Recuperacion / rollback / replay"]
  Recover --> Healthy
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
    if page.doc_id == "arquitectura/capability-map":
        return """
<section><h2>Lectura del mapa</h2><p>Este mapa no asigna ownership por persona o equipo porque esa informacion no aparece verificada en el repo. En su lugar, documenta <strong>ownership tecnico observable</strong>: el paquete o superficie de codigo que hoy concentra la responsabilidad principal de cada capacidad. Cuando exista un owner humano o de squad aprobado, esta pagina debe ampliarse sin borrar esta trazabilidad tecnica.</p></section>
<section><h2>Capacidades principales y ownership tecnico observable</h2><table><thead><tr><th>Capacidad</th><th>Ownership tecnico observable</th><th>Descripcion verificable</th><th>Modulo relacionado</th></tr></thead><tbody><tr><td>Configuracion y arranque</td><td><code>app.main</code>, <code>app.config</code>, <code>app.common</code></td><td>Resuelve modos, valida opciones, carga configuracion y arranca el ciclo principal.</td><td><a href="../desarrollo/environment-configuration.html">Guia de Entorno y Configuracion</a> y <a href="../modulos/control-plane-ops.html">Modulo Plano de Control / Operaciones</a></td></tr><tr><td>Market data e ingestion</td><td><code>app.ingestion</code>, <code>app.marketdata</code></td><td>Captura, valida, normaliza, deduplica, persiste y recupera datos de mercado y metadata de instrumentos.</td><td><a href="../modulos/ingestion.html">Modulo Ingestion</a> y <a href="../modulos/exchange-adapter.html">Modulo Adaptador de Exchange</a></td></tr><tr><td>Feature engineering</td><td><code>app.features</code></td><td>Construye features, mantiene estado cuantitativo, cache, auditoria y persistencia asociada.</td><td><a href="../modulos/feature-store-feature-engine.html">Modulo Feature Store / Feature Engine</a></td></tr><tr><td>Decision cuantitativa</td><td><code>app.strategy</code>, <code>app.risk</code></td><td>Transforma <code>FeatureVector</code> en senales y despues en intenciones filtradas por riesgo.</td><td><a href="../modulos/research.html">Modulo Research</a> y <a href="../modulos/risk-engine.html">Modulo Motor de Riesgo</a></td></tr><tr><td>Ejecucion y estado</td><td><code>app.execution</code>, <code>app.portfolio</code></td><td>Materializa la ruta paper visible en el repo y consolida el estado de portfolio.</td><td><a href="../modulos/execution.html">Modulo Ejecucion</a> y <a href="../modulos/portfolio.html">Modulo Portfolio</a></td></tr><tr><td>Ops y observabilidad</td><td><code>app.ops</code>, <code>app.observability</code></td><td>Emite evidencias operativas, readiness, release gates, logging y soporte de promocion.</td><td><a href="../operacion/service-catalog.html">Catalogo de Servicios</a> y <a href="../modulos/control-plane-ops.html">Modulo Plano de Control / Operaciones</a></td></tr></tbody></table></section>
<section><h2>Trazabilidad hacia modulos del portal</h2><table><thead><tr><th>Capacidad</th><th>Paginas del portal que la desarrollan</th><th>Fuentes del repo visibles</th></tr></thead><tbody><tr><td>Market data e ingestion</td><td><a href="../modulos/ingestion.html">Modulo Ingestion</a>, <a href="../flujos/market-data-flow.html">Flujo de Datos de Mercado</a>, <a href="../datos/contratos-event-schemas.html">Contratos y Esquemas de Eventos</a></td><td><code>app.ingestion/</code>, <code>app.marketdata/</code>, <code>docs/ingestion.md</code></td></tr><tr><td>Feature engineering</td><td><a href="../modulos/feature-store-feature-engine.html">Modulo Feature Store / Feature Engine</a>, <a href="../research-simulation/workflow-research.html">Flujo de Trabajo de Investigacion</a></td><td><code>app.features/</code>, <code>docs/architecture.md</code></td></tr><tr><td>Decision cuantitativa</td><td><a href="../flujos/strategy-signal-to-execution.html">Flujo de Senal de Estrategia a Ejecucion</a>, <a href="../dominio-trading/riesgo.html">Riesgo</a></td><td><code>app.strategy/</code>, <code>app.risk/</code></td></tr><tr><td>Ejecucion y estado</td><td><a href="../flujos/order-lifecycle.html">Ciclo de Vida de Ordenes</a>, <a href="../modulos/execution.html">Modulo Ejecucion</a>, <a href="../modulos/portfolio.html">Modulo Portfolio</a></td><td><code>app.execution/</code>, <code>app.portfolio/</code></td></tr><tr><td>Ops y observabilidad</td><td><a href="../operacion/runbooks.html">Runbooks Operativos</a>, <a href="../operacion/deployment-release.html">Despliegue y Liberacion</a>, <a href="../arquitectura/observabilidad.html">Observabilidad</a></td><td><code>app.ops/</code>, <code>app.observability/</code>, <code>docs/operations/</code>, <code>docs/ops/</code></td></tr></tbody></table></section>
<section><h2>Criterio de ownership usado en esta pagina</h2><ul><li>Si la capacidad esta claramente concentrada en uno o varios paquetes del repo, esos paquetes se listan como ownership tecnico observable.</li><li>Si una capacidad cruza varias capas, se documenta como ownership compartido por paquetes y no como un unico owner artificial.</li><li>Esta pagina no asigna ownership de persona, squad o guardia operativa porque esa definicion no esta verificada en las fuentes revisadas.</li></ul></section>
<section><h2>Informacion pendiente</h2><ul><li>Confirmar ownership humano o de equipo para cada capacidad antes de convertir este mapa en una matriz RACI.</li><li>Completar si existe una frontera operativa distinta entre <code>app.ops</code> y <code>app.observability</code> mas alla de la agrupacion tecnica observable.</li><li>Verificar si existe un ejecutor live adicional que cambie el alcance de la capacidad de ejecucion respecto al paper path hoy visible.</li></ul></section>
"""
    if page.doc_id == "modulos/control-plane-ops":
        return """
<section><h2>Lectura verificada del modulo</h2><p>El modulo visible hoy no es un backoffice conectado al runtime en caliente, sino un <strong>control plane web interno</strong> desacoplado para operar ingestion. El patron implementado es <code>Hot Path -&gt; JSONL Telemetry Sink -&gt; Read Model SQLite -&gt; FastAPI/Jinja2 -&gt; UI</code>, con un worker separado para comandos asincronos.</p></section>
<section><h2>Responsabilidades y limites</h2><table><thead><tr><th>Responsabilidad</th><th>Superficie verificada</th><th>Limite explicito</th></tr></thead><tbody><tr><td>Servir paginas de operacion</td><td><code>app.controlplane.api</code> + plantillas Jinja/HTMX</td><td>No expone buffers, caches ni objetos vivos del runtime principal.</td></tr><tr><td>Materializar un read model</td><td><code>app.controlplane.builder.ReadModelBuilder</code></td><td>Solo consume JSONL de telemetria y refs de checkpoint; no consulta el hot path directamente.</td></tr><tr><td>Persistir vistas y cola</td><td><code>ControlPlaneStore</code>, <code>SQLiteControlPlaneStore</code></td><td>La base de datos es del control plane; no se usa como storage del engine.</td></tr><tr><td>Ejecutar acciones operativas</td><td><code>app.controlplane.worker</code> + <code>operations.py</code></td><td>Solo encola y procesa <code>ack_alert</code>, <code>resync_stream</code> y <code>replay_range</code>; no hace start/stop del proceso de ingestion.</td></tr><tr><td>Preparar migracion futura</td><td><code>PostgresControlPlaneStore</code> reservado en <code>store.py</code></td><td>La implementacion PostgreSQL aun no existe; solo esta preparado el contrato.</td></tr></tbody></table></section>
<section><h2>Componentes verificados</h2><table><thead><tr><th>Componente</th><th>Archivos principales</th><th>Rol</th></tr></thead><tbody><tr><td>API web</td><td><code>app/controlplane/api.py</code></td><td>Recibe requests HTML/JSON, llama a <code>sync_once()</code> y renderiza las vistas.</td></tr><tr><td>Builder del read model</td><td><code>app/controlplane/builder.py</code></td><td>Consume <code>ingestion_summary</code>, <code>ingestion_health</code>, <code>operational_alert</code> y <code>checkpoint_audit_ref</code>.</td></tr><tr><td>Store abstracto y backend SQLite</td><td><code>app/controlplane/store.py</code>, <code>app/controlplane/sqlite_store.py</code>, <code>app/controlplane/store_factory.py</code></td><td>Materializa tablas <code>runs</code>, <code>stream_status</code>, <code>alerts</code>, <code>checkpoint_summary</code>, <code>command_requests</code>, <code>command_audit</code> y <code>read_model_offsets</code>.</td></tr><tr><td>Worker</td><td><code>app/controlplane/worker.py</code></td><td>Reclama comandos pendientes, ejecuta operaciones y actualiza auditoria y telemetria.</td></tr><tr><td>Operaciones</td><td><code>app/controlplane/operations.py</code></td><td>Implementa <code>ack_alert</code>, <code>resync_stream</code> y <code>replay_range</code> reutilizando surfaces ya presentes del repo.</td></tr><tr><td>Frontend HTML</td><td><code>app/controlplane/templates/</code></td><td>Renderiza pantallas internas con Jinja2 y formularios HTMX.</td></tr></tbody></table></section>
<section><h2>Flujo de lectura de la UI</h2><ol><li>El operador entra por <code>/ui/overview</code> o cualquier otra ruta HTML servida por <code>webUI</code>.</li><li>Antes de consultar la vista, la API llama a <code>builder.sync_once()</code>.</li><li>El builder procesa solo lineas nuevas de los JSONL gracias a <code>read_model_offsets</code>.</li><li>Los eventos nuevos se materializan en SQLite del control plane.</li><li>La plantilla Jinja consulta el snapshot agregado desde SQLite y renderiza HTML.</li></ol><p>Este flujo mantiene la UI sobre un <strong>read model derivado</strong> y evita inspeccionar memoria, dedup windows o colas del ingestion engine.</p></section>
<section><h2>Flujo de comandos asincronos</h2><ol><li>La UI hace <code>POST</code> a <code>/api/commands/ack-alert</code>, <code>/api/commands/resync</code> o <code>/api/commands/replay</code>.</li><li>La API inserta una fila <code>pending</code> en <code>command_requests</code> y anade auditoria de encolado.</li><li><code>control-plane-worker</code> reclama el siguiente comando, lo marca <code>running</code> y ejecuta la operacion correspondiente.</li><li>Al terminar, el worker marca <code>succeeded</code> o <code>failed</code> y persiste <code>command_audit</code>.</li><li>El worker emite ademas <code>recovery_command_audit</code> en el telemetry path para trazabilidad externa.</li></ol></section>
<section><h2>Pantallas verificadas de la UI v1</h2><table><thead><tr><th>Pagina</th><th>Ruta</th><th>Que muestra hoy</th></tr></thead><tbody><tr><td>Overview</td><td><code>/ui/overview</code></td><td>Tarjetas de runs, streams, degraded streams, open alerts, pending commands y checkpoint summaries, mas tablas de runs recientes, streams degradados y alertas abiertas.</td></tr><tr><td>Runs</td><td><code>/ui/runs</code></td><td>Listado de ejecuciones materializadas con modo, resultado, updated_at y resumen corto.</td></tr><tr><td>Run detail</td><td><code>/ui/runs/{run_id}</code></td><td>JSON completo de <code>summary</code> y <code>health</code> del run seleccionado.</td></tr><tr><td>Streams</td><td><code>/ui/streams</code></td><td>Estado por scope, run asociado y ultimo timestamp visto.</td></tr><tr><td>Stream detail</td><td><code>/ui/streams/{scope}</code></td><td>Payload JSON de metrics materializado para ese scope.</td></tr><tr><td>Alerts</td><td><code>/ui/alerts</code></td><td>Alertas abiertas o acked, con formulario <code>Ack</code> para las abiertas.</td></tr><tr><td>Checkpoints</td><td><code>/ui/checkpoints</code></td><td>Resumen por stream key con cursor, ultimo evento y ruta del checkpoint.</td></tr><tr><td>Recovery Center</td><td><code>/ui/recovery</code></td><td>Formularios para <code>Resync Stream</code>, <code>Replay Raw Range</code> y <code>Replay Quarantine</code>.</td></tr><tr><td>Audit</td><td><code>/ui/audit</code></td><td>Historial de eventos de <code>command_audit</code> del control plane.</td></tr></tbody></table></section>
<section><h2>Dependencias y despliegue observables</h2><ul><li><code>docker-compose.yml</code> define dos servicios de control plane separados: <code>webUI</code> y <code>control-plane-worker</code>.</li><li>Ambos comparten bind mount del repo y el volumen <code>.venv</code> con el servicio <code>app</code>.</li><li><code>webUI</code> publica <code>8001:8001</code> y ejecuta <code>python -m app.controlplane.api</code>.</li><li><code>control-plane-worker</code> ejecuta <code>python -m app.controlplane.worker</code> y consume la cola SQLite.</li><li>La relacion entre engine, UI y worker es principalmente por filesystem compartido: JSONL de telemetria, refs de checkpoints y SQLite de control plane.</li></ul></section>
<section><h2>Huecos conocidos</h2><ul><li>Las plantillas hablan de polling, pero la v1 no implementa aun auto-refresh HTMX por <code>hx-get</code>; el refresco principal sigue siendo por navegacion o recarga.</li><li>No hay autenticacion fuerte, RBAC ni multiusuario; el valor por defecto del operador es <code>ui-operator</code>.</li><li>La implementacion PostgreSQL aun es un stub reservado y no puede desplegarse.</li><li>No se exponen logs live ni raw market data desde la UI, por diseno.</li></ul></section>
"""
    if page.doc_id == "arquitectura/deployment":
        return """
<section><h2>Lectura de despliegue verificada</h2><p>El repo muestra un despliegue local basado en <code>Dockerfile</code> + <code>docker-compose.yml</code> con tres servicios visibles: <code>app</code>, <code>webUI</code> y <code>control-plane-worker</code>. La UI de ingestion no se sirve desde el contenedor principal, sino desde un contenedor separado que comparte filesystem con el engine y solo consume telemetria y el read model desacoplado.</p></section>
<section><h2>Unidades desplegables verificadas</h2><table><thead><tr><th>Unidad</th><th>Superficie verificada</th><th>Responsabilidad</th><th>Notas de despliegue</th></tr></thead><tbody><tr><td>Imagen base de aplicacion</td><td><code>Dockerfile</code></td><td>Construye una imagen Python 3.11 slim con Poetry y toolchain minimo.</td><td>Base compartida por <code>app</code>, <code>webUI</code> y <code>control-plane-worker</code>.</td></tr><tr><td>Servicio <code>app</code></td><td><code>docker-compose.yml</code> servicio <code>app</code></td><td>Runtime persistente del repo para ejecutar ingestion, tests y tooling operativo.</td><td>Usa <code>container_name=mytradersystem-app</code> y queda en <code>tail -f /dev/null</code> como contenedor base de trabajo.</td></tr><tr><td>Servicio <code>webUI</code></td><td><code>docker-compose.yml</code> servicio <code>webUI</code></td><td>Sirve el control plane web interno de ingestion.</td><td>Publica <code>8001:8001</code> y ejecuta <code>python -m app.controlplane.api --env dev --host 0.0.0.0 --port 8001</code>.</td></tr><tr><td>Servicio <code>control-plane-worker</code></td><td><code>docker-compose.yml</code> servicio <code>control-plane-worker</code></td><td>Consume la cola de comandos y ejecuta acciones operativas asincronas.</td><td>Ejecuta <code>python -m app.controlplane.worker --env dev</code> sin exponer puertos.</td></tr><tr><td>Ejecucion directa en host</td><td><code>python -m app</code>, <code>python -m app.controlplane.api</code>, scripts PowerShell y CLI</td><td>Permite correr engine, UI o worker sin contenedor cuando el entorno local ya esta preparado.</td><td>No reemplaza al despliegue Compose; es una alternativa de operacion y desarrollo.</td></tr></tbody></table></section>
<section><h2>Entornos visibles en el repo</h2><table><thead><tr><th>Entorno</th><th>Archivo de configuracion</th><th>data_dir visible</th><th>Observacion</th></tr></thead><tbody><tr><td>dev</td><td><code>config.dev.yaml</code></td><td><code>./data/dev</code></td><td>Entorno local de desarrollo y validacion manual.</td></tr><tr><td>test</td><td><code>config.test.yaml</code></td><td><code>./data/test</code></td><td>Entorno orientado a pruebas y menor ruido de logging.</td></tr><tr><td>prod</td><td><code>config.prod.yaml</code></td><td><code>/var/lib/mytradersystem/data/prod</code></td><td>Perfil de produccion visible en configuracion, pero no se documenta aqui un despliegue distribuido o un orquestador adicional porque no estan verificados en el repo.</td></tr></tbody></table></section>
<section><h2>Proceso de despliegue local verificado</h2><ol><li><code>scripts/docker-build.ps1</code> y <code>docker compose build</code> construyen la imagen base.</li><li><code>docker compose up -d app control-plane-worker webUI</code> levanta los tres servicios visibles.</li><li><code>webUI</code> escucha en <code>http://127.0.0.1:8001/ui/overview</code> y comparte bind mount del repo con los otros dos contenedores.</li><li>Dentro de los contenedores, el repo queda montado en <code>/workspace</code> y el virtualenv se reutiliza en <code>/workspace/.venv</code>.</li></ol></section>
<section><h2>Relaciones de despliegue</h2><ul><li>Los tres servicios comparten el mismo bind mount del repo y el volumen <code>poetry_venv</code>.</li><li><code>app</code> es quien puede producir market data, checkpoints y telemetria del runtime principal.</li><li><code>webUI</code> y <code>control-plane-worker</code> comparten el telemetry path <code>data/&lt;env&gt;/ui-telemetry</code> y la base <code>data/&lt;env&gt;/control-plane/control_plane.sqlite</code>.</li><li>Solo <code>webUI</code> expone un puerto hacia el operador; el worker no publica interfaces de red.</li><li>La conectividad externa verificada en configuracion sigue apuntando a Binance por <code>ws_base</code> y <code>rest_base</code>; el control plane no reemplaza esa integracion.</li></ul></section>
<section><h2>Informacion pendiente</h2><ul><li>No se ha verificado un manifiesto de Kubernetes, ECS, Nomad o similar.</li><li>No se ha verificado una pipeline CI/CD que materialice despliegue automatizado mas alla de scripts locales y Compose.</li><li>No se ha verificado una separacion de volmenes distinta entre engine y control plane; la iteracion actual usa filesystem compartido del workspace.</li><li>No se ha verificado si <code>prod</code> se ejecuta dentro del mismo patron Compose, en host directo o en otra topologia externa.</li></ul></section>
"""
    if page.doc_id == "arquitectura/topologia-runtime":
        return """
<section><h2>Lectura runtime verificada</h2><p>La topologia runtime ya no es solo el proceso Python principal: el repo muestra un <strong>runtime cuantitativo principal</strong> mas un <strong>control plane web separado</strong>. El engine sigue siendo el unico que procesa market data y el control plane se limita a leer telemetria append-only y a escribir sobre un read model SQLite desacoplado.</p></section>
<section><h2>Procesos y entrypoints visibles</h2><table><thead><tr><th>Proceso o entrypoint</th><th>Superficie verificada</th><th>Rol runtime</th></tr></thead><tbody><tr><td>CLI principal</td><td><code>python -m app</code>, <code>app.__main__</code>, <code>app.main.run()</code></td><td>Punto de entrada general para ciclos dry, paper y live.</td></tr><tr><td>Servicio de ingestion</td><td><code>app.ingestion.service.run_ingestion_service(...)</code></td><td>Ejecuta captura, validacion, deduplicacion y persistencia de market data sin recorrer el resto del pipeline cuantitativo.</td></tr><tr><td>Control plane web</td><td><code>python -m app.controlplane.api</code></td><td>Sincroniza el read model y sirve HTML/JSON para Overview, Runs, Streams, Alerts, Checkpoints, Recovery y Audit.</td></tr><tr><td>Worker del control plane</td><td><code>python -m app.controlplane.worker</code></td><td>Consume la cola de comandos y ejecuta <code>ack_alert</code>, <code>resync_stream</code> y <code>replay_range</code> fuera del request HTTP.</td></tr><tr><td>Scripts operativos</td><td><code>scripts/ingestion_*.py</code>, PowerShell Docker, Makefile</td><td>Disparan validaciones, pruebas y arranque local de los contenedores.</td></tr></tbody></table></section>
<section><h2>Nodos y superficies runtime</h2><table><thead><tr><th>Nodo o superficie</th><th>Materializacion visible</th><th>Relacion runtime</th></tr></thead><tbody><tr><td>Browser del operador</td><td>Navegador accediendo a <code>http://127.0.0.1:8001/ui/overview</code></td><td>Solo habla con <code>webUI</code>.</td></tr><tr><td>Servicio <code>webUI</code></td><td>FastAPI + Jinja2 + HTMX</td><td>Hace <code>sync_once()</code>, lee SQLite y renderiza vistas; no consulta el hot path.</td></tr><tr><td>Servicio <code>control-plane-worker</code></td><td>Loop Python separado</td><td>Consume la cola SQLite y ejecuta recovery commands.</td></tr><tr><td>Telemetry path</td><td><code>data/&lt;env&gt;/ui-telemetry/*.jsonl</code></td><td>Bus append-only entre engine, builder y worker.</td></tr><tr><td>Read model DB</td><td><code>data/&lt;env&gt;/control-plane/control_plane.sqlite</code></td><td>Fuente exclusiva de lectura de la UI y cola/auditoria del control plane.</td></tr><tr><td>Engine principal</td><td>Proceso Python en host o dentro de <code>app</code></td><td>Produce market data, checkpoints y telemetria; no expone estado interno por HTTP.</td></tr><tr><td>Proveedor externo</td><td>Binance REST y WS</td><td>Entrega market data live, historico y metadata de instrumentos.</td></tr></tbody></table></section>
<section><h2>Relaciones runtime verificadas</h2><ul><li><code>webUI</code> llama a <code>ReadModelBuilder.sync_once()</code> antes de cada vista para materializar lineas nuevas de telemetria en SQLite.</li><li>El builder solo procesa <code>ingestion_summary</code>, <code>ingestion_health</code>, <code>operational_alert</code> y <code>checkpoint_audit_ref</code>.</li><li><code>control-plane-worker</code> y <code>webUI</code> coordinan por <code>command_requests</code> y <code>command_audit</code> dentro del mismo SQLite de control plane.</li><li>La comunicacion observable entre contenedores es principalmente por filesystem compartido; no se ha verificado un RPC HTTP entre <code>webUI</code> y el servicio <code>app</code>.</li><li>El hot path de ingestion sigue fuera de la UI: dedup, persistencia y handoff viven en el engine principal.</li></ul></section>
<section><h2>Topologia por modo</h2><table><thead><tr><th>Modo</th><th>Topologia observada</th><th>Diferencia relevante</th></tr></thead><tbody><tr><td>dry</td><td>Proceso Python principal, sin IO externo obligatorio, con control plane opcional encima.</td><td>Apto para pruebas y CI; la UI puede mostrar poco estado si no hay telemetria.</td></tr><tr><td>paper</td><td>Engine con market data live y ejecucion paper, mas control plane desacoplado.</td><td>La UI ayuda a operar ingestion y recovery sin entrar en memoria del engine.</td></tr><tr><td>live</td><td>Proceso principal mas estricto, con el mismo patron de control plane desacoplado.</td><td>El repo endurece validaciones, pero no muestra una topologia distribuida distinta del control plane.</td></tr></tbody></table></section>
<section><h2>Huecos conocidos</h2><ul><li>No se ha verificado un scheduler, cola, bus de mensajes o microservicio independiente entre ingestion y el resto del pipeline; el control plane no cambia ese hecho.</li><li>No se ha verificado una topologia multi instancia con balanceo o HA para <code>webUI</code> o el worker.</li><li>La v1 sigue apoyandose en SQLite y filesystem compartido; PostgreSQL esta solo reservado como backend futuro.</li></ul></section>
"""
    if page.doc_id == "arquitectura/dependencias-servicios":
        return """
<section><h2>Lectura de dependencias verificada</h2><p>La vista de dependencias observable en el repo no corresponde a una malla de microservicios independientes. Lo verificado hoy es un conjunto de paquetes Python fuertemente acoplados por el entrypoint <code>app.main</code>, con dependencias externas concretas hacia Binance, filesystem local y librerias de runtime como <code>httpx</code>, <code>websockets</code> y <code>pyarrow</code>.</p></section>
<section><h2>Inventario de dependencias y criticidad</h2><table><thead><tr><th>Dependencia o superficie</th><th>Tipo</th><th>Criticidad</th><th>Uso verificado</th><th>Efecto esperado si falla</th></tr></thead><tbody><tr><td><code>app.config</code></td><td>Interna</td><td>Alta</td><td>Carga configuracion, resuelve entorno y parametros de runtime.</td><td>El proceso principal no arranca o arranca con configuracion insegura.</td></tr><tr><td><code>app.ingestion</code> + <code>app.marketdata</code></td><td>Interna</td><td>Alta</td><td>Captura, valida, normaliza y persiste market data y metadata.</td><td>Se corta la entrada de datos y el pipeline paper/live queda sin eventos fiables.</td></tr><tr><td><code>app.features</code></td><td>Interna</td><td>Alta</td><td>Construye <code>FeatureVector</code> y mantiene estado cuantitativo.</td><td>Strategy y risk no reciben insumos consistentes; el ciclo cuantitativo se interrumpe.</td></tr><tr><td><code>app.strategy</code> + <code>app.risk</code></td><td>Interna</td><td>Alta</td><td>Transforman features en senales y despues en intenciones filtradas.</td><td>El sistema puede quedarse sin decisiones o producir intenciones invalidas.</td></tr><tr><td><code>app.execution.paper</code> + <code>app.portfolio.state</code></td><td>Interna</td><td>Media</td><td>Materializan ejecucion paper y estado de portfolio visible en el repo.</td><td>El ciclo puede ingerir y calcular, pero no consolidar fills, cash ni posiciones.</td></tr><tr><td><code>app.observability</code> + <code>app.ops</code></td><td>Interna</td><td>Media</td><td>Logging, release gates, readiness y evidencias operativas.</td><td>La plataforma pierde visibilidad y validaciones operativas, pero parte del pipeline puede seguir ejecutando.</td></tr><tr><td>Binance REST y WS</td><td>Externa</td><td>Alta</td><td>Proveedor verificado para market data live, historico y metadata de instrumentos.</td><td>Sin proveedor no hay live market data, snapshots de metadata ni backfill desde esa fuente.</td></tr><tr><td>Filesystem local + <code>pyarrow</code></td><td>Mixta</td><td>Alta</td><td>Persistencia Parquet, checkpoints, snapshots y evidencias de validacion.</td><td>Falla la persistencia, se degrada recovery y pueden perderse checkpoints o artefactos.</td></tr><tr><td><code>httpx</code> y <code>websockets</code></td><td>Externa de runtime</td><td>Alta</td><td>Transporte REST y WS para ingestion y metadata.</td><td>Se degrada o bloquea el acceso a feeds y endpoints remotos.</td></tr><tr><td><code>pytest</code></td><td>Externa de desarrollo</td><td>Baja</td><td>Validacion automatizada en entorno de desarrollo y CI.</td><td>No bloquea runtime, pero reduce capacidad de detectar regresiones antes de desplegar.</td></tr></tbody></table></section>
<section><h2>Dependencias internas y acoplamientos relevantes</h2><table><thead><tr><th>Relacion</th><th>Acoplamiento verificado</th><th>Impacto</th></tr></thead><tbody><tr><td><code>app.main</code> -&gt; pipeline cuantitativo</td><td>El entrypoint importa y coordina ingestion, features, strategy, risk, execution y portfolio.</td><td>Concentra el wiring principal; un cambio incompatible puede romper todo el ciclo.</td></tr><tr><td><code>app.ingestion.runner</code> / <code>storage</code> / <code>resilience</code></td><td><code>docs/dependencies.md</code> los identifica como acoplamiento fuerte para live y persistencia.</td><td>Un fallo en storage o resilience degrada captura, deduplicacion o durabilidad.</td></tr><tr><td><code>app.common.dto</code> -&gt; resto de modulos</td><td>Los DTO se usan como contrato tipado compartido entre modulos.</td><td>Cambios incompatibles en DTO impactan de forma transversal.</td></tr><tr><td><code>app.config</code> -&gt; CLIs y runtime</td><td>Las claves y validaciones compartidas gobiernan modos, rutas y politicas.</td><td>Errores de config afectan arranque, seguridad operativa y rutas de datos.</td></tr><tr><td><code>app.observability.logger</code> -&gt; runtime y ops</td><td>Logging y trazabilidad estan conectados a entrypoint, ingestion y tooling.</td><td>Su fallo no siempre tumba el pipeline, pero reduce seriamente el diagnostico.</td></tr></tbody></table></section>
<section><h2>Fallos esperables por clase de dependencia</h2><ul><li>Si falla Binance o cambia su API, ingestion pierde continuidad, el backfill puede degradarse y los snapshots de instrumentos quedan bloqueados o desalineados.</li><li>Si falla <code>pyarrow</code> o la ruta de salida no es escribible, el sistema pierde persistencia Parquet, checkpoints y parte de su capacidad de recovery.</li><li>Si falla <code>app.features</code>, el pipeline puede seguir recibiendo eventos, pero no generar insumos utilizables para strategy y risk.</li><li>Si falla <code>app.execution.paper</code> o <code>app.portfolio.state</code>, la ruta visible en el repo deja de cerrar el ciclo de decision con estado consolidado.</li><li>Si falla observabilidad, el sistema puede seguir corriendo parcialmente, pero empeoran MTTR, trazabilidad y confianza operativa.</li></ul></section>
<section><h2>Informacion pendiente</h2><ul><li>No se ha verificado en el repo una base de datos, un broker de mensajes o un scheduler externo que deban entrar en esta matriz de dependencias.</li><li>No se ha verificado una integracion live de ejecucion distinta de <code>paper_execute</code>.</li><li>No se ha verificado una matriz formal de SLA o ownership humano por dependencia; aqui solo se documenta criticidad tecnica observable.</li></ul></section>
"""
    if page.doc_id == "arquitectura/observabilidad":
        return """
<section><h2>Lectura de observabilidad verificada</h2><p>La observabilidad visible en el repo se apoya en tres superficies principales: <strong>logs JSON estructurados</strong>, <strong>metricas y summaries de ingestion</strong> y <strong>alertas operativas con severidad y accion recomendada</strong>. Ademas, los artefactos de readiness, canary, soak y release gates actuan como evidencia viva para promotion y diagnostico.</p></section>
<section><h2>Senales principales</h2><table><thead><tr><th>Senal</th><th>Superficie verificada</th><th>Uso operativo</th></tr></thead><tbody><tr><td>Logs estructurados</td><td><code>app.observability.logger.JsonFormatter</code></td><td>Emite JSON con <code>ts</code>, <code>level</code>, <code>logger</code>, <code>module</code>, <code>message</code> y <code>trace_id</code> cuando existe.</td></tr><tr><td>Alertas operativas</td><td><code>app.observability.alerts</code></td><td>Modela alertas como <code>reconnect_storm</code>, <code>gap_irreparable</code>, <code>heartbeat_missed</code>, <code>sink_failure</code> o <code>compaction_failure_detected</code>.</td></tr><tr><td>Summaries de ingestion</td><td><code>ingestion summary</code> y <code>ingestion health</code> documentados en runbook y runtime</td><td>Consolidan estado del ciclo, gaps, duplicados, latencia, reconnects y degradacion por stream.</td></tr><tr><td>Storage health</td><td><code>app.ingestion.storage_health</code></td><td>Mide backlog de segmentos, lag de compactacion, fallos de compactacion y filas normalizadas.</td></tr><tr><td>Release/readiness reports</td><td><code>app.ops.readiness_orchestrator</code> y <code>app.ops.release_gates</code></td><td>Convierten multiples checks en estados <code>PASS</code>, <code>FAIL</code> y bloques requeridos o informativos.</td></tr></tbody></table></section>
<section><h2>Metricas y thresholds verificables</h2><table><thead><tr><th>Metrica</th><th>Threshold paper</th><th>Threshold live</th><th>Interpretacion</th></tr></thead><tbody><tr><td><code>reconnects_total</code></td><td>warning 3 / critical 5</td><td>warning 2 / critical 3</td><td>Volumen anormal de reconnects del stream.</td></tr><tr><td><code>exchange_receive_skew_seconds</code></td><td>warning 5 / critical 30</td><td>warning 2 / critical 10</td><td>Skew entre tiempo del exchange y recepcion local.</td></tr><tr><td><code>receive_process_skew_seconds</code></td><td>warning 1 / critical 5</td><td>warning 0.5 / critical 2</td><td>Retraso entre recepcion y procesamiento efectivo.</td></tr><tr><td><code>processing_latency_seconds</code></td><td>warning 1 / critical 5</td><td>warning 0.5 / critical 2</td><td>Edad del evento cuando el pipeline lo acepta.</td></tr><tr><td><code>compaction_lag_seconds</code></td><td>warning 300 / critical 900</td><td>warning 120 / critical 300</td><td>Backlog operativo de compactacion en normalized storage.</td></tr><tr><td><code>duplicates_total</code>, <code>gaps_total</code>, <code>gap_irreparable_total</code>, <code>heartbeat_missed_total</code>, <code>invalid_timestamp_total</code>, <code>compaction_failures_total</code></td><td>0 / 0</td><td>0 / 0</td><td>Cualquier ocurrencia es señal de degradacion o fallo segun contexto.</td></tr></tbody></table></section>
<section><h2>Logs y trazabilidad</h2><ul><li>Los logs son JSON y redaccion campos sensibles por clave o patron de valor.</li><li><code>trace_id</code> se propaga via <code>ContextVar</code> y se anade a cada log cuando esta presente.</li><li>El logger puede emitir a stdout y a fichero rotado, registrando ademas <code>log_file_metrics</code> sobre tamano y backups.</li><li>La forma canonica de alerta es <code>message = operational alert</code> con <code>alert_type</code>, <code>alert_severity</code>, <code>observed</code>, <code>threshold</code> y <code>recommended_action</code>.</li></ul></section>
<section><h2>Informacion pendiente</h2><ul><li>No se ha verificado una plataforma externa de dashboards o trazas distribuidas.</li><li>No se ha verificado una exportacion Prometheus, OpenTelemetry o similar fuera de los contratos y reportes locales visibles.</li><li>La observabilidad documentada aqui es la que existe en codigo y artefactos locales; si hay tooling externo adicional, no aparece en el repo revisado.</li></ul></section>
"""
    if page.doc_id == "arquitectura/health-model":
        return """
<section><h2>Lectura del modelo de salud verificado</h2><p>El repo no expone un endpoint HTTP de health tradicional. En su lugar, el modelo de salud observable se construye combinando <strong>readiness reports</strong>, <strong>release gates</strong>, <strong>ingestion health</strong>, <strong>storage health</strong> y alertas operativas. El estado resultante no es binario puro; hay una degradacion intermedia antes del fallo total.</p></section>
<section><h2>Estados de salud documentados</h2><table><thead><tr><th>Estado</th><th>Criterio observable</th><th>Consecuencia</th></tr></thead><tbody><tr><td>Saludable</td><td>Canaries, replay parity, storage y observability contract en PASS; sin alertas criticas.</td><td>El runtime es confiable para operar segun el modo documentado.</td></tr><tr><td>Ready</td><td><code>ReadinessReport.pass_ok = true</code> y release gates sin bloques requeridos en FAIL.</td><td>La promocion o ejecucion objetivo puede continuar.</td></tr><tr><td>Degradado</td><td>Hay warning operativos, backlog de compactacion, skew alto, reconnect storms o drift que no rompen aun todos los gates.</td><td>Se puede seguir ejecutando de forma condicionada, pero con menor confianza y mayor necesidad de vigilancia.</td></tr><tr><td>No ready / Fail</td><td>Readiness FAIL, release gates FAIL, gap irreparable, sink failure, compaction failure o exact recovery no verificado cuando es requerido.</td><td>No debe promoverse ni asumirse continuidad saludable del sistema afectado.</td></tr></tbody></table></section>
<section><h2>Senales de readiness y liveness</h2><table><thead><tr><th>Categoria</th><th>Senales verificadas</th><th>Uso</th></tr></thead><tbody><tr><td>Readiness</td><td><code>ReadinessReport.overall_status</code>, <code>pass_ok</code>, resultados por paso en <code>run_ingestion_readiness</code></td><td>Determina si el sistema esta listo para el target <code>paper</code> o <code>live</code>.</td></tr><tr><td>Liveness de streams</td><td><code>heartbeat_missed_total</code>, <code>reconnects_total</code>, <code>gap_irreparable_total</code>, alertas <code>heartbeat_missed</code> y <code>reconnect_storm</code></td><td>Evalua si el conector sigue vivo y si el feed mantiene continuidad minima.</td></tr><tr><td>Salud de persistencia</td><td><code>segments_pending_total</code>, <code>compaction_lag_seconds</code>, <code>compaction_failures_total</code></td><td>Evalua si normalized storage sigue siendo fiable para recovery y consumo.</td></tr><tr><td>Salud semantica</td><td><code>shadow_semantic_diff</code>, vendor contracts, replay parity, exact recovery</td><td>Evita considerar sano un pipeline que sigue vivo pero produce datos semanticos incorrectos.</td></tr></tbody></table></section>
<section><h2>Criterios de degradacion</h2><ul><li><strong>Warning</strong>: reconnects, skew o anomalies por encima de threshold warning implican degradacion controlada y vigilancia reforzada.</li><li><strong>Critical</strong>: thresholds critical o alertas de severidad error implican estado no confiable para promotion o continuidad sin intervencion.</li><li><strong>Storage degradado</strong>: backlog alto o compaction lag elevado pueden permitir continuidad temporal, pero reducen confianza para sesiones largas o recovery.</li><li><strong>Fail inmediato</strong>: <code>sink_failure</code>, <code>gap_irreparable</code>, <code>compaction_failure_detected</code> o gates requeridos en FAIL deben tratarse como no ready.</li></ul></section>
<section><h2>Informacion pendiente</h2><ul><li>No se ha verificado un endpoint de readiness/liveness expuesto por HTTP o un orchestrator externo que consuma estos estados.</li><li>No se ha verificado una taxonomia global de estados fuera de ingestion y release gating.</li><li>Si existen health checks para execution live o servicios externos no visibles en el repo, no pueden documentarse aun.</li></ul></section>
"""
    if page.doc_id == "operacion/service-catalog":
        return """
<section><h2>Lectura operativa verificada</h2><p>El catalogo visible ya no se limita al engine principal. Hoy el despliegue local muestra tres servicios Compose con papeles distintos: <code>app</code> como runtime base, <code>webUI</code> como interfaz operativa interna y <code>control-plane-worker</code> como ejecutor asincrono de comandos del plano de control.</p></section>
<section><h2>Servicios y superficies operativas verificadas</h2><table><thead><tr><th>Servicio o superficie</th><th>Tipo</th><th>Responsabilidad</th><th>Interfaz visible</th><th>Efecto si falla</th></tr></thead><tbody><tr><td><code>app</code></td><td>Contenedor base / runtime</td><td>Aloja el repo, el engine y el tooling para ejecutar ingestion, tests y operaciones manuales.</td><td>CLI y shell dentro del contenedor.</td><td>Sin el engine no hay produccion de telemetria, checkpoints ni market data procesada.</td></tr><tr><td><code>webUI</code></td><td>Servicio HTTP interno</td><td>Sirve la UI de ingestion y sincroniza el read model antes de renderizar vistas.</td><td><code>http://127.0.0.1:8001/ui/*</code></td><td>Se pierde visibilidad operativa web, pero el engine puede seguir ejecutando.</td></tr><tr><td><code>control-plane-worker</code></td><td>Worker asincrono</td><td>Consume la cola de comandos y ejecuta <code>ack_alert</code>, <code>resync_stream</code> y <code>replay_range</code>.</td><td>Sin puerto expuesto; loop sobre SQLite.</td><td>La UI puede seguir leyendo, pero las acciones de recovery quedan pendientes.</td></tr><tr><td>Telemetry path</td><td>Superficie compartida</td><td>Recoge JSONL append-only de summaries, health, alertas y auditoria de recovery.</td><td><code>data/&lt;env&gt;/ui-telemetry/</code></td><td>Degrada el read model y la visibilidad; por diseno no debe tumbar ingestion.</td></tr><tr><td>Read model SQLite</td><td>Base del control plane</td><td>Materializa runs, streams, alerts, checkpoints, command queue y audit.</td><td><code>data/&lt;env&gt;/control-plane/control_plane.sqlite</code></td><td>Sin esta base la UI no puede consultar ni encolar acciones, aunque el hot path siga vivo.</td></tr></tbody></table></section>
<section><h2>Comandos operativos expuestos por la UI</h2><table><thead><tr><th>Comando</th><th>Endpoint</th><th>Ejecutor</th><th>Auditoria</th></tr></thead><tbody><tr><td><code>ack_alert</code></td><td><code>POST /api/commands/ack-alert</code></td><td><code>execute_ack_alert</code></td><td><code>command_requests</code> + <code>command_audit</code></td></tr><tr><td><code>resync_stream</code></td><td><code>POST /api/commands/resync</code></td><td><code>execute_resync_stream</code></td><td><code>command_requests</code> + <code>command_audit</code> + <code>recovery_command_audit</code></td></tr><tr><td><code>replay_range</code></td><td><code>POST /api/commands/replay</code></td><td><code>execute_replay_range</code></td><td><code>command_requests</code> + <code>command_audit</code> + <code>recovery_command_audit</code></td></tr></tbody></table></section>
<section><h2>Interacciones operativas verificadas</h2><ul><li>El operador solo entra por <code>webUI</code>; no hay interfaz HTTP directa hacia <code>app</code>.</li><li><code>webUI</code> y el worker comparten la DB SQLite del control plane para cola y auditoria.</li><li>El engine y el control plane se coordinan mediante filesystem compartido y JSONL append-only, no mediante consultas al hot path.</li><li>La recuperacion lanzada desde la UI es asincrona y queda visible en <code>Audit</code>.</li></ul></section>
<section><h2>Runbooks y ownership tecnico observable</h2><table><thead><tr><th>Servicio</th><th>Ownership tecnico observable</th><th>Runbook o referencia</th></tr></thead><tbody><tr><td><code>app</code></td><td><code>app.ingestion</code>, <code>app.marketdata</code>, <code>app.ops</code></td><td><code>docs/operations/ingestion_runbook.md</code> y checklist de rollback/promotion.</td></tr><tr><td><code>webUI</code></td><td><code>app.controlplane.api</code>, <code>app.controlplane.builder</code>, <code>app.controlplane.templates</code></td><td><a href="../modulos/control-plane-ops.html">Modulo Plano de Control / Operaciones</a>.</td></tr><tr><td><code>control-plane-worker</code></td><td><code>app.controlplane.worker</code>, <code>app.controlplane.operations</code></td><td><a href="../modulos/control-plane-ops.html">Modulo Plano de Control / Operaciones</a> y <a href="troubleshooting.html">Resolucion de Problemas</a>.</td></tr></tbody></table></section>
<section><h2>Informacion pendiente</h2><ul><li>No se ha verificado una operativa multiusuario, RBAC o aprobaciones para comandos criticos.</li><li>No se ha verificado un dashboard externo de observabilidad del control plane; la v1 opera sobre HTML, SQLite y JSONL locales.</li><li>No se ha verificado un backend PostgreSQL ni una cola externa para el control plane.</li></ul></section>
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
        "<section class=\"status-card status-card-footer\"><h2>Estado de la pagina</h2><dl>"
        "<dt>Estado</dt><dd>Base creada</dd>"
        "<dt>Informacion pendiente</dt><dd><ul>"
        "<li>Completar detalles reales de la aplicacion.</li>"
        "<li>Validar fuentes de verdad y responsables.</li>"
        "</ul></dd>"
        f"<dt>Ultima actualizacion</dt><dd>{escape(GENERATED_AT)}</dd></dl></section>"
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


def search_scope_options(current: Page) -> str:
    options = ['<option value="">Todos</option>']
    for section, meta in SECTIONS.items():
        if not section:
            continue
        options.append(f'<option value="{escape(section)}">{escape(meta["name"])}</option>')
    return "".join(options)


def search_suggestions(current: Page) -> str:
    links = []
    for suggestion in SEARCH_SUGGESTIONS:
        href = f'{escape(relpath(current, SEARCH_PAGE.url))}?q={escape(suggestion)}'
        links.append(f'<a class="search-chip" href="{href}">{escape(suggestion)}</a>')
    return "".join(links)


def search_cards(current: Page) -> str:
    cards = []
    for section in SEARCH_CARD_SECTIONS:
        meta = SECTIONS[section]
        page_count = len(meta["pages"])
        href = f'{escape(relpath(current, SEARCH_PAGE.url))}?scope={escape(section)}'
        cards.append(
            '<article class="card search-category-card"><div class="search-card-head">%s<div><h3>%s</h3><p>%s paginas</p></div></div><a class="button" href="%s">Explorar</a></article>'
            % (
                icon(current, SECTION_ICON_MAP.get(section, "document"), cls="ui-icon ui-icon-card"),
                escape(meta["name"]),
                page_count,
                href,
            )
        )
    return "".join(cards)


def search_results_shell() -> str:
    return (
        '<section class="search-results-section" data-search-root data-search-backend="static" data-search-index="assets/search-index.json">'
        '<div class="section-heading"><p class="section-kicker">Busqueda documental</p><h2>Resultados</h2>'
        '<p class="search-results-meta" data-search-meta>Introduce una consulta para explorar la documentacion.</p></div>'
        '<div class="search-results-list" data-search-results></div>'
        '<div class="search-empty-state" data-search-empty>Usa la barra superior para buscar por titulo, seccion o contenido.</div>'
        "</section>"
    )


def metadata(page: Page) -> str:
    return (
        f"<!-- doc_id: {page.doc_id} -->\n"
        "<!-- version_doc: 0.1.0 -->\n"
        "<!-- estado: Base creada -->\n"
        f"<!-- ultima_actualizacion: {GENERATED_AT} -->\n"
        f"<!-- ruta_canonica_publicacion: {CANONICAL_PUBLICATION_ROUTE} -->\n"
        f"<!-- ruta_workspace_actual: {CURRENT_WORKSPACE_ROUTE} -->\n"
        "<!-- secciones_clave: Proposito, Contenido inicial, Placeholders estructurados -->"
    )


def render_search_page() -> str:
    page = SEARCH_PAGE
    return "\n".join(
        [
            "<!DOCTYPE html>",
            '<html lang="es">',
            "<head>",
            '  <meta charset="utf-8" />',
            '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
            f"  <title>{escape(page.title)}</title>",
            f'  <link rel="stylesheet" href="{escape(relpath(page, "assets/styles.css"))}" />',
            f"  {metadata(page)}",
            "</head>",
            "<body>",
            f"  {header_html(page)}",
            '  <main class="layout">',
            '    <section class="hero search-hero">',
            '      <p class="eyebrow">Busqueda documental</p>',
            '      <h1>Como podemos ayudarte?</h1>',
            f'      <p class="breadcrumbs"><a href="{escape(relpath(page, "index.html"))}">Inicio</a> / <span>Busqueda</span></p>',
            '      <p class="lead">Busca respuestas en arquitectura, operacion, modulos y runbooks sin salir del portal.</p>',
            f'      <form class="search-page-form" action="{escape(relpath(page, page.url))}" method="get" role="search">',
            f'        <div class="search-page-shell">{icon(page, "search", cls="ui-icon ui-icon-search-page")}'
            '          <input class="search-page-input" type="search" name="q" placeholder="Buscar en la documentacion" autocomplete="off" data-search-query-input />'
            f'          <label class="sr-only" for="search-scope-select">Filtrar por seccion</label><select id="search-scope-select" class="search-page-scope" name="scope" data-search-scope-select>{search_scope_options(page)}</select>'
            f'          <button class="site-search-submit search-page-submit" type="submit">{icon(page, "arrow-right", cls="ui-icon ui-icon-submit", label="Buscar")}</button>'
            "        </div>",
            "      </form>",
            f'      <div class="search-suggestions-inline"><span>Sugeridas:</span>{search_suggestions(page)}</div>',
            "    </section>",
            '    <section><div class="section-heading"><p class="section-kicker">Accesos directos</p><h2>Explora por seccion</h2></div><div class="card-grid">',
            f"      {search_cards(page)}",
            "    </div></section>",
            f"    {search_results_shell()}",
            f"    {status_panel()}",
            "  </main>",
            '  <footer class="site-footer">',
            "    <p>Base documental HTML estatica preparada para actualizaciones incrementales.</p>",
            f"    <p>Ultima generacion: {escape(GENERATED_AT)}</p>",
            "  </footer>",
            f'  <script src="{escape(relpath(page, "assets/app.js"))}"></script>',
            "</body>",
            "</html>",
        ]
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
            f"  {header_html(page)}",
            "  <main class=\"layout\">",
            f"    <section class=\"hero{' hero-home' if page.kind == 'home' else ''}\">",
            hero_content(page),
            "    </section>",
            f"    {publication_note_html}",
            f"    {home_visual_html}",
            f"    {home_cards(page)}",
            f"    {section_page_links(page)}",
            f"    {mermaid_html}",
            f"    {generic_sections(page)}",
            f"    {status_panel()}",
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
            f"    <p>Ultima generacion: {escape(GENERATED_AT)}</p>",
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
  align-items: flex-start;
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
.top-nav {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  align-items: flex-start;
}
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}
.ui-icon {
  width: 1rem;
  height: 1rem;
  flex: 0 0 auto;
  color: currentColor;
}
.nav-link-direct {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.7rem 1rem;
  border-radius: 999px;
  color: var(--muted);
  border: 1px solid transparent;
}
.nav-link-direct.active,
.nav-link-direct:hover {
  background: rgba(13, 74, 116, 0.08);
  color: var(--accent-2);
  border-color: rgba(13, 74, 116, 0.12);
  text-decoration: none;
}
.nav-group {
  position: relative;
  min-width: 0;
}
.nav-group summary {
  list-style: none;
}
.nav-group summary::-webkit-details-marker {
  display: none;
}
.nav-group-summary {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.7rem 1rem;
  border-radius: 999px;
  color: var(--muted);
  border: 1px solid transparent;
  cursor: pointer;
  user-select: none;
  background: transparent;
}
.nav-group-summary::after {
  content: "▾";
  font-size: 0.78rem;
  color: currentColor;
}
.nav-group.active > .nav-group-summary,
.nav-group[open] > .nav-group-summary,
.nav-group-summary:hover {
  background: rgba(13, 74, 116, 0.08);
  color: var(--accent-2);
  border-color: rgba(13, 74, 116, 0.12);
  text-decoration: none;
}
.nav-group-panel {
  position: absolute;
  top: calc(100% + 0.55rem);
  left: 0;
  min-width: 260px;
  display: none;
  grid-template-columns: 1fr;
  gap: 0.35rem;
  padding: 0.8rem;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.98);
  border: 1px solid var(--line);
  box-shadow: 0 22px 50px rgba(17, 33, 52, 0.12);
}
.nav-group[open] > .nav-group-panel {
  display: grid;
}
.nav-sublink {
  display: block;
  padding: 0.65rem 0.8rem;
  border-radius: 12px;
  color: var(--muted);
}
.nav-sublink.active,
.nav-sublink:hover {
  background: rgba(13, 74, 116, 0.08);
  color: var(--accent-2);
  text-decoration: none;
}
.site-search-form {
  margin-left: auto;
  min-width: min(360px, 100%);
}
.site-search-shell {
  display: flex;
  align-items: center;
  gap: 0.65rem;
  min-height: 46px;
  padding: 0.3rem 0.35rem 0.3rem 0.8rem;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: var(--surface-3);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.75);
}
.site-search-input {
  flex: 1 1 auto;
  min-width: 0;
  border: 0;
  background: transparent;
  color: var(--text);
  font-size: 0.96rem;
  outline: none;
}
.site-search-input::placeholder {
  color: #8090a3;
}
.site-search-submit {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  border-radius: 12px;
  border: 1px solid var(--line);
  background: var(--surface);
  color: var(--accent-2);
  cursor: pointer;
}
.site-search-submit:hover {
  background: var(--surface-2);
}
.layout {
  width: 100%;
  margin: 0;
  padding: 2rem clamp(1rem, 2vw, 2rem) 4rem;
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 1.5rem;
}
.layout > * { min-width: 0; }
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
.status-card { position: static; align-self: auto; }
.status-card-footer { margin-top: 0.25rem; }
.status-card dt { font-weight: 700; margin-top: 1rem; }
.status-card dd { margin-left: 0; color: var(--muted); }
.layout > section,
.layout > .card-grid,
.layout > .status-card,
.layout > .publication-note,
.layout > .home-diagram { grid-column: 1 / -1; }
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
  width: 100%;
  margin: 0;
  padding: 0 clamp(1rem, 2vw, 2rem) 2rem;
  color: var(--muted);
}
.search-page-form {
  margin-top: 1rem;
  max-width: 920px;
}
.search-hero {
  padding-block: 1.5rem;
}
.search-hero h1 {
  max-width: none;
  font-size: clamp(1.9rem, 3.6vw, 2.9rem);
  margin-bottom: 0.45rem;
}
.search-hero .lead {
  max-width: 60ch;
  margin: 0;
}
.search-hero .breadcrumbs {
  margin-bottom: 0.65rem;
}
.search-page-shell {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) 240px auto;
  align-items: center;
  gap: 0.65rem;
  min-height: 46px;
  padding: 0.3rem 0.35rem 0.3rem 0.8rem;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: var(--surface-3);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.75);
}
.search-page-input,
.search-page-scope {
  border: 0;
  background: transparent;
  outline: none;
  font-size: 1rem;
  color: var(--text);
}
.search-page-input {
  min-width: 0;
}
.search-page-scope {
  min-height: 40px;
  padding: 0 0.55rem;
  border-left: 1px solid rgba(207, 215, 227, 0.8);
  color: var(--muted);
}
.search-page-submit {
  flex: 0 0 auto;
}
.search-suggestions-inline {
  display: flex;
  flex-wrap: wrap;
  gap: 0.6rem;
  align-items: center;
  margin-top: 0.85rem;
  color: var(--muted);
  font-size: 0.88rem;
}
.search-chip {
  display: inline-flex;
  align-items: center;
  padding: 0.38rem 0.8rem;
  border-radius: 999px;
  background: var(--surface-3);
  color: var(--accent-2);
  border: 1px solid rgba(13, 74, 116, 0.08);
}
.search-chip:hover {
  background: var(--surface-2);
  text-decoration: none;
}
.search-results-section {
  padding: 1.4rem 1.6rem;
}
.search-category-card {
  display: grid;
  gap: 1rem;
}
.search-card-head {
  display: flex;
  align-items: flex-start;
  gap: 1rem;
}
.search-category-card h3 {
  margin: 0 0 0.35rem;
  font-size: 1rem;
}
.search-category-card p {
  margin: 0;
  color: var(--muted);
}
.ui-icon-card {
  width: 2.4rem;
  height: 2.4rem;
  padding: 0.55rem;
  border-radius: 14px;
  background: linear-gradient(135deg, rgba(94, 77, 255, 0.12), rgba(58, 216, 174, 0.16));
  color: #5e4dff;
}
.search-results-meta {
  margin: 0.2rem 0 0;
  color: var(--muted);
}
.search-results-list {
  display: grid;
  gap: 1rem;
  margin-top: 1.25rem;
}
.search-result-card {
  border: 1px solid var(--line);
  border-radius: calc(var(--radius) - 4px);
  background:
    linear-gradient(180deg, rgba(246, 248, 251, 0.98), rgba(255, 255, 255, 1)),
    linear-gradient(180deg, rgba(13, 74, 116, 0.04), transparent);
  padding: 1rem 1.1rem;
}
.search-result-card h3 {
  margin: 0;
}
.search-result-card h3 a {
  color: var(--accent-2);
}
.search-result-card p {
  margin: 0.5rem 0 0;
  color: var(--muted);
}
.search-result-meta {
  display: inline-flex;
  margin-top: 0.7rem;
  color: var(--accent);
  font-size: 0.82rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.search-empty-state {
  margin-top: 1rem;
  padding: 1rem 1.1rem;
  border: 1px dashed var(--line);
  border-radius: calc(var(--radius) - 4px);
  background: var(--surface-3);
  color: var(--muted);
}
.mermaid { overflow-x: auto; }
@media (max-width: 960px) {
  .hero-home-grid { grid-template-columns: 1fr; }
  .hero-metrics { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .search-page-shell {
    grid-template-columns: auto minmax(0, 1fr);
  }
  .search-page-scope {
    grid-column: 1 / -1;
    border-left: 0;
    border-top: 1px solid rgba(207, 215, 227, 0.8);
    padding: 0.65rem 0.15rem 0 0.15rem;
  }
}
@media (max-width: 720px) {
  .site-header {
    position: static;
    padding: 1rem;
    flex-direction: column;
    align-items: stretch;
  }
  .top-nav {
    flex-direction: column;
    gap: 0.6rem;
  }
  .site-search-form {
    margin-left: 0;
    min-width: 0;
  }
  .nav-group {
    width: 100%;
  }
  .nav-group-summary {
    width: 100%;
    justify-content: space-between;
    border-radius: 16px;
    border-color: rgba(13, 74, 116, 0.12);
    background: rgba(255, 255, 255, 0.82);
  }
  .nav-group-panel {
    position: static;
    min-width: 0;
    margin-top: 0.5rem;
    box-shadow: none;
    border-radius: 16px;
  }
  .nav-link-direct {
    width: 100%;
    border-radius: 16px;
    border-color: rgba(13, 74, 116, 0.12);
    background: rgba(255, 255, 255, 0.82);
  }
  .site-search-shell {
    width: 100%;
  }
  .layout { padding-top: 1rem; }
  .hero { padding: 1.4rem; }
  .hero h1 { max-width: none; }
  .hero-metrics { grid-template-columns: 1fr; }
  .hero-actions { flex-direction: column; align-items: stretch; }
  .diagram-lightbox-dialog { width: min(calc(100% - 1rem), 1500px); margin: 0.5rem auto; }
  .search-page-shell {
    grid-template-columns: 1fr;
  }
  .search-page-scope {
    border-left: 0;
    border-top: 1px solid rgba(207, 215, 227, 0.8);
    padding: 0.65rem 0 0;
  }
  .search-suggestions-inline {
    align-items: flex-start;
    flex-direction: column;
  }
}
"""


def app_js() -> str:
    return """
const navGroups = Array.from(document.querySelectorAll(".nav-group"));
const lightbox = document.querySelector(".diagram-lightbox");
const lightboxImage = lightbox?.querySelector(".diagram-lightbox-image");
const lightboxCaption = lightbox?.querySelector(".diagram-lightbox-caption");
const lightboxClose = lightbox?.querySelector(".diagram-lightbox-close");
const searchRoot = document.querySelector("[data-search-root]");

function closeNavGroups(exceptGroup = null) {
  navGroups.forEach((group) => {
    if (group !== exceptGroup) {
      group.removeAttribute("open");
    }
  });
}

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

navGroups.forEach((group) => {
  group.addEventListener("toggle", () => {
    if (group.open) {
      closeNavGroups(group);
    }
  });
});

lightboxClose?.addEventListener("click", closeDiagramLightbox);
lightbox?.addEventListener("click", (event) => {
  const target = event.target;
  if (target instanceof HTMLElement && target.hasAttribute("data-close-lightbox")) {
    closeDiagramLightbox();
  }
});

document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  if (!target.closest(".top-nav")) {
    closeNavGroups();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeNavGroups();
  }
  if (event.key === "Escape" && lightbox && !lightbox.hidden) {
    closeDiagramLightbox();
  }
});

function normalizeSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\\u0300-\\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\\s/-]/g, " ")
    .replace(/\\s+/g, " ")
    .trim();
}

function tokenizeQuery(value) {
  return normalizeSearchText(value)
    .split(" ")
    .filter((token) => token.length > 1);
}

function scoreDocument(documentItem, query, tokens, scope) {
  if (scope && documentItem.section !== scope) {
    return null;
  }
  const title = normalizeSearchText(documentItem.title);
  const summary = normalizeSearchText(documentItem.summary);
  const excerpt = normalizeSearchText(documentItem.excerpt);
  const body = normalizeSearchText(documentItem.body);
  const headings = (documentItem.headings || []).map((heading) => normalizeSearchText(heading)).join(" ");
  const haystack = [title, summary, excerpt, body, headings].join(" ");
  if (!query && !scope) {
    return null;
  }
  if (tokens.length && !tokens.every((token) => haystack.includes(token))) {
    return null;
  }
  let score = 0;
  if (query && title.includes(query)) score += title === query ? 140 : 100;
  tokens.forEach((token) => {
    if (title.includes(token)) score += 20;
    if (headings.includes(token)) score += 10;
    if (summary.includes(token)) score += 6;
    if (excerpt.includes(token) || body.includes(token)) score += 2;
  });
  if (scope && documentItem.section === scope) score += 3;
  return score > 0 ? score : null;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;");
}

async function runStaticSearch() {
  if (!searchRoot) return;
  const params = new URLSearchParams(window.location.search);
  const query = params.get("q") || "";
  const scope = params.get("scope") || "";
  const normalizedQuery = normalizeSearchText(query);
  const tokens = tokenizeQuery(query);
  document.querySelectorAll('input[name="q"]').forEach((input) => {
    input.value = query;
  });
  document.querySelectorAll('select[name="scope"]').forEach((select) => {
    select.value = scope;
  });

  const meta = searchRoot.querySelector("[data-search-meta]");
  const resultsContainer = searchRoot.querySelector("[data-search-results]");
  const emptyState = searchRoot.querySelector("[data-search-empty]");
  const indexUrl = searchRoot.getAttribute("data-search-index");
  if (!meta || !resultsContainer || !emptyState || !indexUrl) return;

  const response = await fetch(indexUrl, { cache: "no-store" });
  const documents = await response.json();
  const results = documents
    .map((documentItem) => ({
      documentItem,
      score: scoreDocument(documentItem, normalizedQuery, tokens, scope),
    }))
    .filter((item) => item.score !== null)
    .sort((left, right) => right.score - left.score || left.documentItem.title.localeCompare(right.documentItem.title) || left.documentItem.url.localeCompare(right.documentItem.url))
    .map((item) => item.documentItem);

  if (!query && !scope) {
    meta.textContent = "Introduce una consulta o filtra por seccion para explorar la documentacion.";
    resultsContainer.innerHTML = "";
    emptyState.hidden = false;
    return;
  }

  if (!results.length) {
    meta.textContent = "No hay resultados para la busqueda actual.";
    resultsContainer.innerHTML = "";
    emptyState.hidden = false;
    return;
  }

  emptyState.hidden = true;
  meta.textContent = `${results.length} resultado(s) para "${query || "scope"}"${scope ? ` en ${scope}` : ""}.`;
  resultsContainer.innerHTML = results
    .map((item) => `
      <article class="card search-result-card">
        <h3><a href="${escapeHtml(item.url)}">${escapeHtml(item.title)}</a></h3>
        <p>${escapeHtml(item.excerpt || item.summary)}</p>
        <span class="search-result-meta">${escapeHtml(item.section_name)} · ${escapeHtml(item.url)}</span>
      </article>
    `)
    .join("");
}

runStaticSearch().catch((error) => {
  if (!searchRoot) return;
  const meta = searchRoot.querySelector("[data-search-meta]");
  const emptyState = searchRoot.querySelector("[data-search-empty]");
  if (meta) meta.textContent = "No se pudo cargar el indice de busqueda.";
  if (emptyState) emptyState.hidden = false;
  console.error(error);
});
"""


def icons_svg() -> str:
    return """<svg xmlns="http://www.w3.org/2000/svg" style="display:none">
  <symbol id="home" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 11.5 12 4l9 7.5"/><path d="M5.5 10.5V20h13V10.5"/><path d="M9.5 20v-5h5v5"/></symbol>
  <symbol id="modules" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="7" height="7" rx="2"/><rect x="13" y="4" width="7" height="7" rx="2"/><rect x="4" y="13" width="7" height="7" rx="2"/><rect x="13" y="13" width="7" height="7" rx="2"/></symbol>
  <symbol id="portal" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3c4.971 0 9 4.029 9 9"/><path d="M12 21c-4.971 0-9-4.029-9-9"/><path d="M12 3c-2.5 2.8-4 5.8-4 9s1.5 6.2 4 9"/><path d="M12 3c2.5 2.8 4 5.8 4 9s-1.5 6.2-4 9"/><path d="M3 12h18"/></symbol>
  <symbol id="architecture" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 18h16"/><path d="M6 18V9l6-4 6 4v9"/><path d="M9 18v-4h6v4"/></symbol>
  <symbol id="flow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="7" height="5" rx="1.5"/><rect x="14" y="5" width="7" height="5" rx="1.5"/><rect x="8.5" y="14" width="7" height="5" rx="1.5"/><path d="M10 7.5h4"/><path d="M17.5 10v1.5c0 1.4-1.1 2.5-2.5 2.5h-3"/></symbol>
  <symbol id="data" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="6" rx="7" ry="3"/><path d="M5 6v6c0 1.7 3.1 3 7 3s7-1.3 7-3V6"/><path d="M5 12v6c0 1.7 3.1 3 7 3s7-1.3 7-3v-6"/></symbol>
  <symbol id="research" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="6"/><path d="m20 20-4.2-4.2"/><path d="M11 8v6"/><path d="M8 11h6"/></symbol>
  <symbol id="support" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12a8 8 0 1 1 16 0"/><path d="M5 12v4a2 2 0 0 0 2 2h1v-6H7a2 2 0 0 0-2 2"/><path d="M19 12v4a2 2 0 0 1-2 2h-1v-6h1a2 2 0 0 1 2 2"/><path d="M12 20h2"/></symbol>
  <symbol id="operations" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1 1 0 0 0 .2 1.1l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9V20a2 2 0 1 1-4 0v-.2a1 1 0 0 0-.6-.9 1 1 0 0 0-1.1.2l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6H4a2 2 0 1 1 0-4h.2a1 1 0 0 0 .9-.6 1 1 0 0 0-.2-1.1l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1 1 0 0 0 1.1.2 1 1 0 0 0 .6-.9V4a2 2 0 1 1 4 0v.2a1 1 0 0 0 .6.9 1 1 0 0 0 1.1-.2l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1 1 0 0 0-.2 1.1 1 1 0 0 0 .9.6H20a2 2 0 1 1 0 4h-.2a1 1 0 0 0-.9.6"/></symbol>
  <symbol id="development" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="m8 16-4-4 4-4"/><path d="m16 8 4 4-4 4"/><path d="m14 4-4 16"/></symbol>
  <symbol id="security" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 5 6v6c0 4.5 2.8 7.7 7 9 4.2-1.3 7-4.5 7-9V6l-7-3Z"/><path d="M9.5 12.5 11 14l3.5-3.5"/></symbol>
  <symbol id="decision" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h10"/><path d="M4 12h16"/><path d="M4 18h8"/><circle cx="18" cy="6" r="2"/><circle cx="14" cy="18" r="2"/></symbol>
  <symbol id="glossary" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 5.5A2.5 2.5 0 0 1 7.5 3H20v18H7.5A2.5 2.5 0 0 0 5 23z"/><path d="M5 5.5V21"/><path d="M9 7h7"/><path d="M9 11h7"/></symbol>
  <symbol id="faq" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9.1 9a3 3 0 1 1 5.8 1c0 2-2.9 2.3-2.9 4"/><path d="M12 18h.01"/><circle cx="12" cy="12" r="9"/></symbol>
  <symbol id="postmortem" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 20h12"/><path d="M8 20V8h8v12"/><path d="M10 4h4"/><path d="M9 12h6"/></symbol>
  <symbol id="template" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><path d="M8 8h8"/><path d="M8 12h8"/><path d="M8 16h5"/></symbol>
  <symbol id="document" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M7 3h7l5 5v13H7z"/><path d="M14 3v6h6"/><path d="M9 13h6"/><path d="M9 17h6"/></symbol>
  <symbol id="search" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></symbol>
  <symbol id="arrow-right" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="m13 6 6 6-6 6"/></symbol>
</svg>
"""


def ensure_docs_dirs() -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    (DOCS / "assets").mkdir(parents=True, exist_ok=True)


def write_search_assets() -> None:
    ensure_docs_dirs()
    (DOCS / "assets" / "styles.css").write_text(styles(), encoding="utf-8")
    (DOCS / "assets" / "app.js").write_text(app_js(), encoding="utf-8")
    (DOCS / "assets" / "icons.svg").write_text(icons_svg(), encoding="utf-8")


def build_search_index() -> None:
    ensure_docs_dirs()
    (DOCS / "assets" / "search-index.json").write_text(
        json.dumps(search_index_payload(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_search_page() -> None:
    ensure_docs_dirs()
    SEARCH_PAGE.path.write_text(render_search_page(), encoding="utf-8")


def patch_existing_headers() -> None:
    header_pattern = re.compile(r"<header class=\"site-header\">.*?</header>", flags=re.DOTALL)
    for page in PAGES:
        if not page.path.exists():
            continue
        original = page.path.read_text(encoding="utf-8")
        updated = header_pattern.sub(header_html(page), original, count=1)
        if updated != original:
            page.path.write_text(updated, encoding="utf-8")


def build_search_artifacts() -> None:
    write_search_assets()
    build_search_index()
    build_search_page()
    patch_existing_headers()


def parse_build_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generador documental HTML")
    parser.add_argument("--patch-header-all", action="store_true", help="Actualiza solo el header en todos los HTML existentes.")
    parser.add_argument("--build-search", action="store_true", help="Genera search.html, search-index y assets asociados, y parchea headers.")
    parser.add_argument("--build-search-index", action="store_true", help="Genera solo assets/search-index.json.")
    parser.add_argument("--build-search-assets", action="store_true", help="Genera solo styles.css, app.js e icons.svg.")
    return parser.parse_args(argv)


def build() -> None:
    ensure_docs_dirs()
    for section in SECTIONS:
        if section:
            (DOCS / section).mkdir(parents=True, exist_ok=True)
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
    write_search_assets()
    build_search_index()
    build_search_page()
    if HOME_DIAGRAM_SOURCE.exists():
        shutil.copy2(HOME_DIAGRAM_SOURCE, DOCS / "assets" / HOME_DIAGRAM_NAME)


if __name__ == "__main__":
    args = parse_build_args()
    if args.patch_header_all:
        patch_existing_headers()
        print(f"Headers actualizados en {DOCS}")
    elif args.build_search:
        build_search_artifacts()
        print(f"Busqueda parcial generada en {DOCS}")
    elif args.build_search_index:
        build_search_index()
        print(f"Indice de busqueda generado en {DOCS / 'assets' / 'search-index.json'}")
    elif args.build_search_assets:
        write_search_assets()
        print(f"Assets de busqueda generados en {DOCS / 'assets'}")
    else:
        build()
        print(f"Generadas {len(PAGES)} paginas en {DOCS}")
