from __future__ import annotations

import argparse
import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs-html"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sirve la documentacion HTML por HTTP local.")
    parser.add_argument("--host", default="127.0.0.1", help="Host de escucha. Por defecto 127.0.0.1.")
    parser.add_argument("--port", default=8000, type=int, help="Puerto HTTP. Por defecto 8000.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not DOCS.exists():
        raise SystemExit(f"No existe el arbol documental esperado: {DOCS}")

    handler = partial(SimpleHTTPRequestHandler, directory=os.fspath(DOCS))
    server = ThreadingHTTPServer((args.host, args.port), handler)
    url = f"http://{args.host}:{args.port}/index.html"
    print(f"Sirviendo docs-html desde {DOCS}")
    print(f"Abre {url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
