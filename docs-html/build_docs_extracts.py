#!/usr/bin/env python3
"""
Extrae resúmenes de páginas HTML de la documentación para auditoría.

Uso:
    python build_docs_extracts.py --docs-root ./docs --output ./docs-audit/input/docs_extracts.txt

Opciones útiles:
    --max-files 40
    --max-paragraphs 3
    --max-chars-per-paragraph 400
"""

from __future__ import annotations

import argparse
import re
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import List, Optional


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


class SimpleHTMLExtractor(HTMLParser):
    """
    Parser HTML simple para extraer:
    - title
    - headings (h1, h2, h3)
    - párrafos
    - elementos de navegación
    - bloques Mermaid
    """

    def __init__(self) -> None:
        super().__init__()
        self.title: Optional[str] = None
        self.headings: list[tuple[str, str]] = []
        self.paragraphs: list[str] = []
        self.nav_links: list[str] = []
        self.mermaid_blocks: list[str] = []

        self._current_tag: Optional[str] = None
        self._current_attrs: dict[str, str] = {}
        self._buffer: list[str] = []

        self._inside_title = False
        self._inside_heading = False
        self._inside_paragraph = False
        self._inside_nav = False
        self._inside_anchor = False
        self._inside_pre = False
        self._inside_mermaid = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        self._current_tag = tag.lower()
        self._current_attrs = {k.lower(): (v or "") for k, v in attrs}

        if self._current_tag == "title":
            self._inside_title = True
            self._buffer = []
        elif self._current_tag in {"h1", "h2", "h3"}:
            self._inside_heading = True
            self._buffer = []
        elif self._current_tag == "p":
            self._inside_paragraph = True
            self._buffer = []
        elif self._current_tag == "nav":
            self._inside_nav = True
        elif self._current_tag == "a":
            self._inside_anchor = True
            if self._inside_nav:
                self._buffer = []
        elif self._current_tag == "pre":
            self._inside_pre = True
            css_class = self._current_attrs.get("class", "")
            if "mermaid" in css_class.split():
                self._inside_mermaid = True
                self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()

        if tag == "title" and self._inside_title:
            text = self._clean_text("".join(self._buffer))
            if text:
                self.title = text
            self._inside_title = False
            self._buffer = []

        elif tag in {"h1", "h2", "h3"} and self._inside_heading:
            text = self._clean_text("".join(self._buffer))
            if text:
                self.headings.append((tag.upper(), text))
            self._inside_heading = False
            self._buffer = []

        elif tag == "p" and self._inside_paragraph:
            text = self._clean_text("".join(self._buffer))
            if text:
                self.paragraphs.append(text)
            self._inside_paragraph = False
            self._buffer = []

        elif tag == "a" and self._inside_anchor:
            if self._inside_nav:
                text = self._clean_text("".join(self._buffer))
                if text:
                    self.nav_links.append(text)
            self._inside_anchor = False
            self._buffer = []

        elif tag == "nav":
            self._inside_nav = False

        elif tag == "pre" and self._inside_pre:
            if self._inside_mermaid:
                text = self._clean_mermaid("".join(self._buffer))
                if text:
                    self.mermaid_blocks.append(text)
            self._inside_pre = False
            self._inside_mermaid = False
            self._buffer = []

        self._current_tag = None
        self._current_attrs = {}

    def handle_data(self, data: str) -> None:
        if (
            self._inside_title
            or self._inside_heading
            or self._inside_paragraph
            or (self._inside_nav and self._inside_anchor)
            or self._inside_mermaid
        ):
            self._buffer.append(data)

    @staticmethod
    def _clean_text(text: str) -> str:
        text = unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _clean_mermaid(text: str) -> str:
        lines = [line.rstrip() for line in text.strip().splitlines()]
        lines = [line for line in lines if line.strip()]
        return "\n".join(lines).strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extrae resúmenes de páginas HTML para auditoría documental."
    )
    parser.add_argument(
        "--docs-root",
        required=True,
        help="Ruta raíz de la documentación, por ejemplo ./docs",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Archivo de salida, por ejemplo ./docs-audit/input/docs_extracts.txt",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=200,
        help="Máximo número de archivos HTML a procesar.",
    )
    parser.add_argument(
        "--max-paragraphs",
        type=int,
        default=3,
        help="Máximo número de párrafos por archivo.",
    )
    parser.add_argument(
        "--max-chars-per-paragraph",
        type=int,
        default=400,
        help="Máximo de caracteres por párrafo extraído.",
    )
    parser.add_argument(
        "--include-mermaid",
        action="store_true",
        help="Incluir el primer bloque Mermaid si existe.",
    )
    return parser.parse_args()


def should_exclude_dir(path: Path) -> bool:
    return path.name in DEFAULT_EXCLUDE_DIRS or path.name.startswith(".")


def list_html_files(docs_root: Path) -> list[Path]:
    html_files: list[Path] = []
    for path in docs_root.rglob("*.html"):
        if any(should_exclude_dir(parent) for parent in path.parents if parent != docs_root.parent):
            continue
        html_files.append(path)
    return sorted(html_files, key=lambda p: str(p).lower())


def shorten(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def extract_from_html(file_path: Path) -> dict[str, object]:
    parser = SimpleHTMLExtractor()
    content = file_path.read_text(encoding="utf-8", errors="ignore")
    parser.feed(content)

    return {
        "title": parser.title,
        "headings": parser.headings,
        "paragraphs": parser.paragraphs,
        "nav_links": parser.nav_links,
        "mermaid_blocks": parser.mermaid_blocks,
    }


def build_extract_block(
    relative_path: str,
    extracted: dict[str, object],
    max_paragraphs: int,
    max_chars_per_paragraph: int,
    include_mermaid: bool,
) -> str:
    lines: list[str] = [f"[{relative_path}]"]

    title = extracted.get("title")
    if isinstance(title, str) and title:
        lines.append(f"TITLE: {title}")

    headings = extracted.get("headings", [])
    if isinstance(headings, list):
        for tag, text in headings[:8]:
            lines.append(f"{tag}: {text}")

    nav_links = extracted.get("nav_links", [])
    if isinstance(nav_links, list) and nav_links:
        nav_preview = ", ".join(nav_links[:10])
        lines.append(f"NAV: {nav_preview}")

    paragraphs = extracted.get("paragraphs", [])
    if isinstance(paragraphs, list) and paragraphs:
        for paragraph in paragraphs[:max_paragraphs]:
            lines.append(f"P: {shorten(paragraph, max_chars_per_paragraph)}")

    mermaid_blocks = extracted.get("mermaid_blocks", [])
    if include_mermaid and isinstance(mermaid_blocks, list) and mermaid_blocks:
        first_block = mermaid_blocks[0]
        lines.append("MERMAID:")
        lines.append(first_block)

    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    docs_root = Path(args.docs_root).resolve()
    output_file = Path(args.output).resolve()

    if not docs_root.exists():
        raise FileNotFoundError(f"No existe docs-root: {docs_root}")
    if not docs_root.is_dir():
        raise NotADirectoryError(f"docs-root no es un directorio: {docs_root}")

    html_files = list_html_files(docs_root)[: args.max_files]
    blocks: List[str] = []

    for file_path in html_files:
        relative_path = str(file_path.relative_to(docs_root.parent)).replace("\\", "/")
        extracted = extract_from_html(file_path)
        block = build_extract_block(
            relative_path=relative_path,
            extracted=extracted,
            max_paragraphs=args.max_paragraphs,
            max_chars_per_paragraph=args.max_chars_per_paragraph,
            include_mermaid=args.include_mermaid,
        )
        blocks.append(block)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n\n".join(blocks) + "\n", encoding="utf-8")

    print(f"[OK] Extractos generados en: {output_file}")
    print(f"[OK] Archivos procesados: {len(html_files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())