from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOCS_ROOT = ROOT / "docs-html"
CANONICAL_DOCS_ALIAS = ROOT / "docs" / "docs-html"


@dataclass
class SearchSyncResult:
    backend: str
    mode: str
    status: str
    docs_root: str
    artifacts_updated: list[str]
    changed_documents: list[str]
    error: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sincroniza los indices de busqueda documental.")
    parser.add_argument("--backend", default="static", choices=["static", "db", "engine"])
    parser.add_argument("--mode", default="incremental", choices=["incremental", "full"])
    parser.add_argument("--docs-root", default="docs-html")
    parser.add_argument("--changed", default="")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args(argv)


def normalize_docs_root(value: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    return candidate.resolve()


def parse_changed(value: str) -> list[str]:
    if not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def supported_static_root(path: Path) -> bool:
    return path == DEFAULT_DOCS_ROOT.resolve() or path == CANONICAL_DOCS_ALIAS.resolve()


def static_artifacts() -> list[str]:
    return [
        str((DEFAULT_DOCS_ROOT / "assets" / "search-index.json").resolve()),
        str((DEFAULT_DOCS_ROOT / "search.html").resolve()),
        str((DEFAULT_DOCS_ROOT / "assets" / "styles.css").resolve()),
        str((DEFAULT_DOCS_ROOT / "assets" / "app.js").resolve()),
        str((DEFAULT_DOCS_ROOT / "assets" / "icons.svg").resolve()),
    ]


def load_docs_generator():
    module_path = ROOT / "scripts" / "generate_docs_html.py"
    spec = importlib.util.spec_from_file_location("generate_docs_html", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el generador documental desde {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def run_static_backend(mode: str, docs_root: Path, changed_documents: list[str], verbose: bool) -> SearchSyncResult:
    if not supported_static_root(docs_root):
        raise ValueError(
            "El backend static actual solo soporta docs-html del workspace actual "
            "o su alias canonico docs/docs-html."
        )

    docs_gen = load_docs_generator()

    if verbose:
        print(
            f"[docs_search_sync] backend=static mode={mode} docs_root={docs_root}",
            file=sys.stderr,
        )

    # v1 mantiene un unico entrypoint estable aunque internamente siga delegando
    # en el generador actual.
    docs_gen.build_search_artifacts()

    return SearchSyncResult(
        backend="static",
        mode=mode,
        status="ok",
        docs_root=str(docs_root),
        artifacts_updated=static_artifacts(),
        changed_documents=changed_documents,
    )


def not_implemented_backend(backend: str, mode: str, docs_root: Path, changed_documents: list[str]) -> SearchSyncResult:
    return SearchSyncResult(
        backend=backend,
        mode=mode,
        status="not_implemented",
        docs_root=str(docs_root),
        artifacts_updated=[],
        changed_documents=changed_documents,
        error=(
            f"El backend '{backend}' aun no esta implementado. "
            "Usa '--backend static' hasta introducir indexacion en base de datos o motor externo."
        ),
    )


def render_text(result: SearchSyncResult) -> str:
    lines = [
        "docs_search_sync",
        f"backend: {result.backend}",
        f"mode: {result.mode}",
        f"status: {result.status}",
        f"docs_root: {result.docs_root}",
    ]
    if result.artifacts_updated:
        lines.append("artifacts_updated:")
        lines.extend(f"- {artifact}" for artifact in result.artifacts_updated)
    else:
        lines.append("artifacts_updated: []")
    if result.changed_documents:
        lines.append("changed_documents:")
        lines.extend(f"- {doc}" for doc in result.changed_documents)
    else:
        lines.append("changed_documents: []")
    if result.error:
        lines.append(f"error: {result.error}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    docs_root = normalize_docs_root(args.docs_root)
    changed_documents = parse_changed(args.changed)

    try:
        if args.backend == "static":
            result = run_static_backend(args.mode, docs_root, changed_documents, args.verbose)
            exit_code = 0
        else:
            result = not_implemented_backend(args.backend, args.mode, docs_root, changed_documents)
            exit_code = 2
    except Exception as exc:  # pragma: no cover - CLI error path
        result = SearchSyncResult(
            backend=args.backend,
            mode=args.mode,
            status="error",
            docs_root=str(docs_root),
            artifacts_updated=[],
            changed_documents=changed_documents,
            error=str(exc),
        )
        exit_code = 1

    if args.as_json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    else:
        print(render_text(result))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
