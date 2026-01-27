#!/usr/bin/env python3
"""
Fetch a Figma file node via the REST API and optionally emit HTML/CSS.

Design source: https://www.figma.com/design/JPWe8gZd4VPmAvnK6tLha1/hazeydata.ai?node-id=29-92

Requirements:
- FIGMA_ACCESS_TOKEN: Personal access token from https://www.figma.com/settings (scope: file_content:read)
- Run from repo root: python scripts/fetch_figma_node.py

Outputs:
- web/figma_node.json: raw API response (optional, --json-only)
- web/index.html, web/styles.css: generated scaffold (default)
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from pathlib import Path

import requests

FIGMA_FILE_KEY = "JPWe8gZd4VPmAvnK6tLha1"
FIGMA_NODE_ID = "29:92"  # URL uses 29-92; API uses 29:92
WEB_DIR = Path(__file__).resolve().parents[1] / "web"


def figma_rgba(c: dict) -> str:
    """Figma color {r,g,b,a} to CSS rgba."""
    r = int((c.get("r", 0) or 0) * 255)
    g = int((c.get("g", 0) or 0) * 255)
    b = int((c.get("b", 0) or 0) * 255)
    a = c.get("a", 1)
    return f"rgba({r},{g},{b},{a})"


def get_fill_color(node: dict) -> str | None:
    """First solid fill as CSS color, or None."""
    fills = node.get("fills") or []
    for f in fills:
        if f.get("visible", True) and f.get("type") == "SOLID" and "color" in f:
            return figma_rgba(f["color"])
    return None


def emit_css_for_node(node: dict, class_name: str, base_x: float, base_y: float) -> list[str]:
    """Emit CSS rules for a single node (position, size, fill, font)."""
    rules = []
    ab = node.get("absoluteBoundingBox")
    if not ab:
        return rules

    x = ab.get("x", 0) - base_x
    y = ab.get("y", 0) - base_y
    w = ab.get("width", 0)
    h = ab.get("height", 0)

    rules.append(f".{class_name} {{")
    rules.append(f"  position: absolute;")
    rules.append(f"  left: {x:.2f}px;")
    rules.append(f"  top: {y:.2f}px;")
    rules.append(f"  width: {w:.2f}px;")
    rules.append(f"  height: {h:.2f}px;")
    rules.append(f"  box-sizing: border-box;")

    fill = get_fill_color(node)
    if fill:
        rules.append(f"  background: {fill};")

    if node.get("type") == "TEXT":
        style = node.get("style") or {}
        ff = style.get("fontFamily", "inherit")
        fs = style.get("fontSize")
        fw = style.get("fontWeight", 400)
        if fs:
            rules.append(f"  font-family: {ff!r}, sans-serif;")
            rules.append(f"  font-size: {fs}px;")
            rules.append(f"  font-weight: {fw};")
        rules.append(f"  display: flex; align-items: center;")

    rules.append("}")
    return rules


def slug(s: str) -> str:
    """Safe class name from node name/id."""
    s = re.sub(r"[^a-zA-Z0-9_-]", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "node"


def collect_nodes(node: dict, parent: dict | None, out: list[tuple[dict, dict | None]], depth: int = 0) -> None:
    """Flatten node tree (node, parent) for rendering."""
    if depth > 20:
        return
    out.append((node, parent))
    for c in node.get("children") or []:
        collect_nodes(c, node, out, depth + 1)


def build_html_css(doc: dict) -> tuple[str, str]:
    """Build minimal HTML and CSS from Figma root node (API returns it as document)."""
    # doc is the requested node (frame 29-92)
    ab = doc.get("absoluteBoundingBox") or {}
    base_x = ab.get("x", 0)
    base_y = ab.get("y", 0)
    w0 = ab.get("width", 1440)
    h0 = ab.get("height", 900)

    nodes: list[tuple[dict, dict | None]] = []
    collect_nodes(doc, None, nodes)

    css_rules: list[str] = [
        "* { box-sizing: border-box; }",
        "body { margin: 0; font-family: system-ui, sans-serif; }",
        f".figma-root {{ position: relative; width: {w0:.0f}px; min-height: {h0:.0f}px; margin: 0 auto; }}",
    ]
    html_parts: list[str] = ['<div class="figma-root">']

    for i, (n, _) in enumerate(nodes):
        name = n.get("name", "unnamed")
        ntype = n.get("type", "FRAME")
        class_name = f"n_{i}_{slug(name)}"

        for line in emit_css_for_node(n, class_name, base_x, base_y):
            css_rules.append(line)

        if ntype == "TEXT":
            chars = html.escape(n.get("characters", ""))
            html_parts.append(f'<div class="{class_name}">{chars}</div>')
        elif ntype in ("FRAME", "GROUP", "RECTANGLE", "VECTOR", "COMPONENT", "INSTANCE"):
            html_parts.append(f'<div class="{class_name}"></div>')

    html_parts.append("</div>")

    html_body = "\n".join(html_parts)
    css = "\n".join(css_rules)
    return html_body, css


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch Figma node and emit HTML/CSS.")
    ap.add_argument("--json-only", action="store_true", help="Only fetch and write figma_node.json")
    ap.add_argument("--token", default=os.environ.get("FIGMA_ACCESS_TOKEN"), help="Figma access token")
    args = ap.parse_args()

    raw = (args.token or "").strip()
    # Handle paste slip: "token" + "python scripts/..." on same line
    token = raw.split()[0] if raw else ""
    if not token:
        print("Set FIGMA_ACCESS_TOKEN or pass --token. Create one at https://www.figma.com/settings", file=sys.stderr)
        return 1
    if not token.startswith("figd_"):
        print("Token should start with 'figd_'. Check you copied the full token.", file=sys.stderr)
        return 1

    url = f"https://api.figma.com/v1/files/{FIGMA_FILE_KEY}/nodes?ids={FIGMA_NODE_ID}"
    headers = {"X-Figma-Token": token}
    try:
        s = requests.Session()
        s.trust_env = False  # ignore HTTP_PROXY / system proxy (avoids 127.0.0.1:9 etc.)
        r = s.get(url, headers=headers, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        msg = str(e)
        if r.status_code == 403:
            try:
                body = r.json()
                err = body.get("err") or body.get("message") or msg
            except Exception:
                err = msg
            print("Figma API 403 Forbidden.", file=sys.stderr)
            print("", file=sys.stderr)
            print("Common causes:", file=sys.stderr)
            print("  1. Token missing scope → Create a new token at https://www.figma.com/settings", file=sys.stderr)
            print("     and enable 'file_content:read'.", file=sys.stderr)
            print("  2. Invalid or expired token → Regenerate the token and try again.", file=sys.stderr)
            print("  3. No access to this file → Ensure you can open the Figma file in a browser.", file=sys.stderr)
            print("", file=sys.stderr)
            if err and err != msg:
                print(f"  API: {err}", file=sys.stderr)
        else:
            print(f"Figma API error: {e}", file=sys.stderr)
        return 1
    except requests.RequestException as e:
        print("Request failed (network/proxy).", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        return 1

    data = r.json()

    err = data.get("err")
    if err:
        print(f"Figma API error: {err}", file=sys.stderr)
        return 1

    nodes = data.get("nodes", {})
    key = list(nodes.keys())[0] if nodes else None
    if not key:
        print("No nodes in response.", file=sys.stderr)
        return 1

    node_data = nodes[key]
    doc = node_data.get("document", {})

    WEB_DIR.mkdir(parents=True, exist_ok=True)
    json_path = WEB_DIR / "figma_node.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {json_path}")

    if args.json_only:
        return 0

    html_body, css = build_html_css(doc)
    if not html_body:
        print("Could not build HTML from node tree.", file=sys.stderr)
        return 1

    html_full = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>hazeydata.ai</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
{html_body}
</body>
</html>
"""

    (WEB_DIR / "index.html").write_text(html_full, encoding="utf-8")
    (WEB_DIR / "styles.css").write_text(css, encoding="utf-8")
    print(f"Wrote {WEB_DIR / 'index.html'}, {WEB_DIR / 'styles.css'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
