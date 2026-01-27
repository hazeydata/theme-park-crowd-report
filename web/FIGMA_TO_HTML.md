# Figma design → HTML/CSS → live site

This folder is your static site. `index.html` and `styles.css` are **placeholders**. Replace them with output from your Figma design, then deploy.

## Design source

- **Figma**: [hazeydata.ai](https://www.figma.com/design/JPWe8gZd4VPmAvnK6tLha1/hazeydata.ai?node-id=29-92&m=dev) (node `29-92`)
- **Fetch via API**: run `scripts/fetch_figma_node.py` (requires `FIGMA_ACCESS_TOKEN`). See script docstring.

## 1. Export HTML/CSS from Figma

Use one of these (all have free tiers):

| Tool | What to do |
|------|------------|
| **[AutoHTML](https://www.figma.com/community/plugin/1159121531475816753)** | Install in Figma → select your home page frame → Export → pick HTML/CSS. Download the zip, then copy the main HTML and CSS into this folder. |
| **[Figma to HTML and CSS](https://www.figma.com/community/plugin/1128731099343788397)** | Select frame → Run plugin → export. Use the generated HTML and CSS. |
| **[Anima](https://www.figma.com/community/plugin/857346721138427857)** | Select frame → Anima → Export code. Use the HTML/CSS (and `assets/` if it exports images). |
| **Figma Dev Mode** | Open Dev Mode → select elements → copy CSS from the right panel. Manually build `index.html` structure and paste styles into `styles.css`. |

**Recommendation:** For a full-page export, **AutoHTML** or **Figma to HTML and CSS** are fastest. Use **Dev Mode** if you prefer to hand-tune structure and reuse our `web/` layout.

### Fetch via API (alternative)

From repo root, with a [Figma access token](https://www.figma.com/settings) (`file_content:read`):

```powershell
$env:FIGMA_ACCESS_TOKEN = "your-token"
python scripts/fetch_figma_node.py
```

This fetches node `29-92` from the hazeydata.ai file, writes `web/figma_node.json`, and overwrites `web/index.html` + `web/styles.css` with a generated scaffold. Use `--json-only` to only save the raw JSON.

### After export

1. **Replace `index.html`**  
   - Use the exported HTML as your new `index.html`, or merge it into the existing structure (header, main, footer).

2. **Replace or merge `styles.css`**  
   - Either replace `styles.css` with the exported CSS, or append it and keep our `:root` variables if you like them.

3. **Images**  
   - Export images from Figma (or use the `assets/` folder from the plugin). Put them in `web/assets/` and fix paths in HTML (e.g. `assets/hero.png`).

4. **Fonts**  
   - If the design uses custom fonts, add `<link>` tags in `<head>` (Google Fonts, Adobe Fonts, or self-hosted). Update `font-family` in CSS to match.

## 2. Run it locally

Open `index.html` in a browser, or serve the folder:

```powershell
cd web
python -m http.server 8080
```

Then visit **http://localhost:8080**.

## 3. Deploy (go live)

See **[DEPLOY.md](DEPLOY.md)** for GitHub Pages, Vercel, and Netlify. Easiest for this repo: **GitHub Pages** from the `web/` folder.

## If you share your Figma file

- **Public view-only link**: You can use **CodeParrot** (Figma → code) or similar with the link. Paste the generated HTML/CSS here and adjust paths.
- **Screenshots**: Share a screenshot of the home page; we can approximate structure and styles and you refine.

Once your Figma export (or our approximation) is in `web/` and you’ve run the deploy steps, your design will be live.
