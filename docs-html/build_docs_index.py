#!/usr/bin/env python3
"""
Genera un índice textual de la documentación HTML.

Salida esperada:
    /docs
      index.html
      /arquitectura
        contexto.html
        arquitectura-alto-nivel.html
      /modulos
        ingestion.html
        oms.html

Uso:
    python build_docs_index.py --docs-root ./docs --output ./docs-audit/input/docs_index.txt
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable


DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".github",
    "__pycache__",
    "node_modules",
    ".idea",
    ".vscode",
    "dist",
    "build",
}

DEFAULT_INCLUDE_EXTENSIONS = {".html"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un árbol textual de la documentación."
    )
    parser.add_argument(
        "--docs-root",
        required=True,
        help="Ruta raíz de la documentación, por ejemplo ./docs",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Archivo de salida, por ejemplo ./docs-audit/input/docs_index.txt",
    )
    parser.add_argument(
        "--include-non-html",
        action="store_true",
        help="Incluir archivos no HTML en el índice.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help="Profundidad máxima a recorrer. Default: 10",
    )
    return parser.parse_args()


def should_exclude_dir(path: Path) -> bool:
    return path.name in DEFAULT_EXCLUDE_DIRS or path.name.startswith(".")


def should_include_file(path: Path, include_non_html: bool) -> bool:
    if path.name.startswith("."):
        return False
    if include_non_html:
        return True
    return path.suffix.lower() in DEFAULT_INCLUDE_EXTENSIONS


def iter_children_sorted(directory: Path) -> Iterable[Path]:
    dirs = []
    files = []
    for child in directory.iterdir():
        if child.is_dir():
            if not should_exclude_dir(child):
                dirs.append(child)
        else:
            files.append(child)
    yield from sorted(dirs, key=lambda p: p.name.lower())
    yield from sorted(files, key=lambda p: p.name.lower())


def build_tree_lines(
    root: Path,
    include_non_html: bool,
    max_depth: int,
) -> list[str]:
    lines: list[str] = [f"/{root.name}"]

    def walk(directory: Path, depth: int) -> None:
        if depth >= max_depth:
            return

        indent = "  " * (depth + 1)
        for child in iter_children_sorted(directory):
            if child.is_dir():
                lines.append(f"{indent}/{child.name}")
                walk(child, depth + 1)
            else:
                if should_include_file(child, include_non_html):
                    lines.append(f"{indent}{child.name}")

    walk(root, 0)
    return lines


def main() -> int:
    args = parse_args()

    docs_root = Path(args.docs_root).resolve()
    output_file = Path(args.output).resolve()

    if not docs_root.exists():
        raise FileNotFoundError(f"No existe docs-root: {docs_root}")
    if not docs_root.is_dir():
        raise NotADirectoryError(f"docs-root no es un directorio: {docs_root}")

    lines = build_tree_lines(
        root=docs_root,
        include_non_html=args.include_non_html,
        max_depth=args.max_depth,
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] Índice generado en: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())