<!-- response-contract v2 -->
## Response Contract — hard rule, overrides all other formatting guidance

Default reply = ONE sentence naming Fred's next step, then an ask_user_input
selector with 2-4 options. Nothing else.

Hard caps:
- 40 words maximum outside the selector.
- One topic per reply. Found something else? Hold it. Do not append it.
- No headers, no tables, no bullet lists, no code blocks unless Fred asked for code.
- No preamble, no context-setting, no closing summary, no "worth noting".
- Reasoning is internal. Report the decision, never the path to it.

Overrides:
- "expand" or "why" → caps off, that reply only.
- "brief me" → 5 bullets max.

# Theme Park Crowd Report — AI Agent Guide

## Critical: Read Before Writing Code

**Read `docs/PIPELINE_V4_DESIGN.md` first.** It defines the pipeline architecture, data flow, and quality gates.

## The #1 Rule

**All data access uses DuckDB + Parquet.** Never use CSV loops or `load_entity_data()`.

```python
import duckdb
con = duckdb.connect()
df = con.execute(f"""
    SELECT * FROM read_parquet('{output_base}/fact_tables/parquet/*.parquet')
    WHERE entity_code = 'MK01'
""").fetchdf()
```

## Project Overview

- **What:** Theme park wait time predictions (Disney, Universal)
- **Stack:** Python + DuckDB + XGBoost
- **Data:** Parquet fact tables in `/mnt/data/pipeline/fact_tables/parquet/`
- **Dimensions:** CSV in `/mnt/data/pipeline/dimension_tables/`
- **Models:** Per-entity XGBoost in `/mnt/data/pipeline/models/{entity_code}/`

## Key Files

- `pipeline/pipeline.py` — **Main pipeline entry point** (V4, runs daily at 6 AM ET)
- `pipeline/steps/s07_training.py` — Per-entity XGBoost training
- `pipeline/steps/s14_content.py` — Content generation + quality gate for tweets
- `src/processors/encoding.py` — Label encoding
- `src/processors/posted_to_actual.py` — POSTED to ACTUAL conversion (DuckDB)
- `docs/PIPELINE_V4_DESIGN.md` — Governing pipeline spec
- `docs/MODELING_AND_WTI_METHODOLOGY.md` — Full methodology docs
- `SESSION_LOG.md` — Source of truth for current project state

## Memory Limits

Server: 62GB RAM. Chunk by park prefix if processing > 30M rows.
