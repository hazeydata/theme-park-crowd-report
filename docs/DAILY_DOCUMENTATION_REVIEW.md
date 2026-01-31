# Daily documentation review (end of day)

Quick checklist. Run through at end of day (or when wrapping up) to keep docs in sync with the pipeline.

## Checklist

1. **docs/PIPELINE_STATE.md** — “Where we are”
   - Paths (repo, output_base), cron, queue-times, dashboard, commands still accurate?
   - If you changed config, cron, queue-times, or dashboard today → update this file.

2. **README.md** — Main project doc
   - Setup, usage, scheduling (Windows + Linux), monitoring (dashboard), state files.
   - Anything you changed today that affects how someone runs or understands the pipeline?

3. **scripts/README.md** and **config/README.md**
   - New scripts or config options? Paths or examples changed?

4. **PROJECT_STRUCTURE.md**
   - New top-level dirs or major new scripts/processors? Only if something structural changed.

5. **Other docs** (only if you touched the area)
   - LINUX_CRON_SETUP, docs/REFRESH_READINESS, dashboard/README — quick skim if relevant.

**Rule of thumb:** If you changed *where* or *how* the pipeline runs (paths, cron, services, dashboard), update **PIPELINE_STATE** and any doc that describes that flow.

---

*Set a daily reminder (calendar/phone) for “Theme park pipeline: end-of-day doc review” so you don’t forget.*
