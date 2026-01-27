# hazeydata.ai / Theme Park Crowd Report – website

Static site for the project home page. Design source: [hazeydata.ai Figma](https://www.figma.com/design/JPWe8gZd4VPmAvnK6tLha1/hazeydata.ai?node-id=29-92&m=dev) (node `29-92`).

- **[FIGMA_TO_HTML.md](FIGMA_TO_HTML.md)** – Export from Figma (plugins or API script) and drop HTML/CSS here
- **[DEPLOY.md](DEPLOY.md)** – GitHub Pages, Vercel, Netlify

**Fetch from Figma API:**  
`FIGMA_ACCESS_TOKEN=… python scripts/fetch_figma_node.py` (from repo root). Creates `web/figma_node.json`, `web/index.html`, `web/styles.css`. Get a token at [Figma Settings](https://www.figma.com/settings).

**Local preview:** `python -m http.server 8080` (run from `web/`), then open http://localhost:8080.
