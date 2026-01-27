# Deploy the site (go live)

The `web/` folder is a static site: `index.html`, `styles.css`, and optional `assets/`. Deploy it to any static host.

## Option 1: GitHub Pages (recommended)

1. **Settings → Pages** in your GitHub repo.
2. **Source**: Deploy from a branch.
3. **Branch**: `main` (or `master`).
4. **Folder**: `/ (root)` **won’t** work cleanly because the repo root has Python, `julia/`, etc. Use one of these:

   **A) Deploy from `web/` via GitHub Actions**

   - Create `.github/workflows/deploy-pages.yml`:

   ```yaml
   name: Deploy site to GitHub Pages
   on:
     push:
       branches: [main]
   permissions:
     contents: read
     pages: write
     id-token: write
   jobs:
     deploy:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - name: Setup Pages
           uses: actions/configure-pages@v4
         - name: Upload artifact
           uses: actions/upload-pages-artifact@v3
           with:
             path: web
         - name: Deploy
           id: deploy
           uses: actions/deploy-pages@v4
   ```

   - In the repo **Settings → Pages**: set **Source** to **GitHub Actions** (not "Deploy from branch").
   - Push to `main`. The workflow deploys the `web/` folder. Site URL: `https://<user>.github.io/<repo>/`.

   **B) Use `docs/` (simpler, but mixes docs and site)**

   - Copy `web/` contents into `docs/` (e.g. `docs/index.html`, `docs/styles.css`, `docs/assets/`).
   - Settings → Pages → Source: **Deploy from branch** → Branch: `main` → Folder: **`/docs`**.
   - Site: `https://<user>.github.io/<repo>/`. Only do this if you’re okay with docs and site sharing `docs/`.

## Option 2: Vercel

1. Go to [vercel.com](https://vercel.com), import your GitHub repo.
2. **Root Directory**: set to `web`.
3. **Framework Preset**: Other (static).
4. Deploy. Vercel will serve `index.html` at the root.

## Option 3: Netlify

1. [app.netlify.com](https://app.netlify.com) → Add new site → Import from Git.
2. **Base directory**: `web`.
3. **Build command**: leave empty.
4. **Publish directory**: `web` (or `.` if base is already `web`).
5. Deploy.

## Option 4: Any static host

Upload the contents of `web/` (including `assets/` if you use it) to your host. Ensure:

- `index.html` is the index.
- Links to `styles.css` and `assets/` use relative paths (e.g. `styles.css`, `assets/logo.svg`).

---

**Quick local check:** Run `python -m http.server 8080` inside `web/`, open `http://localhost:8080`, then deploy.
