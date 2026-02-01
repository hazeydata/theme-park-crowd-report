"""
Pipeline status dashboard — single vertical scroll, refreshes every 5 minutes.

Shows: pipeline step status, queue-times job status, entities table (this run + obs counts, wait types).
Optional auth: set DASH_USER and DASH_PASSWORD to enable Basic Auth (for sharing with wilma).
Runs on 0.0.0.0 so accessible on LAN.

Usage:
  python dashboard/app.py
  DASH_USER=admin DASH_PASSWORD=secret python dashboard/app.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# Project root and src on path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

import dash
from dash import dcc, html
from dash.dependencies import Input, Output

import pandas as pd

from utils.paths import get_output_base
from utils.pipeline_status import load as load_pipeline_status
from processors.entity_index import get_all_entities

# Optional Basic Auth for sharing
try:
    import dash_auth
except ImportError:
    dash_auth = None

# Output base from config (same as pipeline)
def get_base():
    return Path(get_output_base()).resolve()


def is_queue_times_running() -> bool:
    try:
        r = subprocess.run(
            ["pgrep", "-f", "get_wait_times_from_queue_times"],
            capture_output=True,
            timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


def get_recent_queue_times_sample(base: Path, max_rows: int = 15) -> list[dict]:
    """Load a sample of the most recently collected queue-times from staging (by observed_at from latest file)."""
    staging_dir = base / "staging" / "queue_times"
    if not staging_dir.exists():
        return []
    csvs = list(staging_dir.rglob("*.csv"))
    if not csvs:
        return []
    # Sort by mtime descending (most recently modified first); data is appended so newest is at end of file
    csvs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    all_rows: list[dict] = []
    for path in csvs[:5]:  # check up to 5 most recent files to gather enough rows
        try:
            df = pd.read_csv(path, low_memory=False)
            if df.empty or "entity_code" not in df.columns or "observed_at" not in df.columns:
                continue
            need = ["entity_code", "observed_at"]
            if "wait_time_minutes" in df.columns:
                need.append("wait_time_minutes")
            df = df[[c for c in need if c in df.columns]].dropna(subset=["observed_at"])
            if df.empty:
                continue
            for _, r in df.iterrows():
                wait_val = "—"
                if "wait_time_minutes" in r.index and pd.notna(r.get("wait_time_minutes")):
                    try:
                        wait_val = int(float(r["wait_time_minutes"]))
                    except (ValueError, TypeError):
                        pass
                all_rows.append({
                    "entity": str(r["entity_code"]),
                    "observed_at": str(r["observed_at"]),
                    "wait_min": wait_val,
                })
            if len(all_rows) >= max_rows * 3:  # enough to sort and take recent
                break
        except Exception:
            continue
    if not all_rows:
        return []
    # Sort by observed_at descending and take the most recent max_rows
    try:
        sorted_rows = sorted(all_rows, key=lambda x: x["observed_at"], reverse=True)
    except Exception:
        sorted_rows = all_rows[-max_rows:] if len(all_rows) > max_rows else all_rows
        sorted_rows.reverse()
    return sorted_rows[:max_rows]


def build_layout() -> list:
    base = get_base()
    status_path = base / "state" / "pipeline_status.json"
    index_db = base / "state" / "entity_index.sqlite"

    status = load_pipeline_status(base) if base else {}
    pipeline = status.get("pipeline", {})
    training = status.get("training", {})
    steps = pipeline.get("steps", {})
    started_at = pipeline.get("started_at", "—")
    current_step = pipeline.get("current_step", "—")
    entities_list = training.get("entities", [])
    current_entity = training.get("current_entity")
    current_index = training.get("current_index", 0)
    total_entities = training.get("total", 0)
    training_workers = training.get("workers")
    running_entities = [e.get("code") for e in entities_list if e.get("status") == "running"]

    # Entity index stats (row_count, actual_count, posted_count, priority_count, latest_park_date)
    try:
        index_df = get_all_entities(index_db) if index_db.exists() else None
    except Exception:
        index_df = None

    # Merge training entities with index stats
    rows = []
    for e in entities_list:
        code = e.get("code", "")
        name = e.get("name", code)
        st = e.get("status", "pending")
        row_count = actual = posted = priority = latest = None
        if index_df is not None and not index_df.empty and code in index_df["entity_code"].values:
            r = index_df[index_df["entity_code"] == code].iloc[0]
            row_count = int(r.get("row_count", 0) or 0)
            actual = int(r.get("actual_count", 0) or 0)
            posted = int(r.get("posted_count", 0) or 0)
            priority = int(r.get("priority_count", 0) or 0)
            latest = str(r.get("latest_park_date", ""))
        is_running = st == "running" or (current_entity and code == current_entity) or code in running_entities
        rows.append({
            "Entity": code,
            "Name": name,
            "Status": st,
            "Rows": row_count if row_count is not None else "—",
            "ACTUAL": actual if actual is not None else "—",
            "POSTED": posted if posted is not None else "—",
            "PRIORITY": priority if priority is not None else "—",
            "Latest date": latest or "—",
            "running": is_running,
        })

    # Pipeline steps table
    step_order = ("etl", "dimensions", "aggregates", "report", "training", "forecast", "wti")
    step_labels = {
        "etl": "ETL",
        "dimensions": "Dimension fetches",
        "aggregates": "Posted aggregates",
        "report": "Wait time DB report",
        "training": "Batch training",
        "forecast": "Forecast",
        "wti": "WTI",
    }
    step_rows = []
    for s in step_order:
        st = steps.get(s, {})
        lab = step_labels.get(s, s)
        stat = st.get("status", "pending")
        done_at = st.get("done_at") or st.get("failed_at") or "—"
        step_rows.append(html.Tr([
            html.Td(lab, style={"padding": "4px 12px"}),
            html.Td(stat, style={"padding": "4px 12px", "fontWeight": "bold"}),
            html.Td(done_at[:19] if isinstance(done_at, str) and len(done_at) > 19 else str(done_at), style={"padding": "4px 12px"}),
        ]))

    queue_running = is_queue_times_running()

    layout = [
        html.H1("Pipeline status", style={"marginBottom": "8px"}),
        html.P(f"Output base: {base}", style={"fontSize": "14px", "color": "#666", "marginBottom": "16px"}),
        html.P(f"Last updated: {status.get('last_updated', '—')}", style={"fontSize": "12px", "color": "#999"}),

        html.H2("Pipeline", style={"marginTop": "24px", "marginBottom": "8px"}),
        html.P(f"Started: {started_at[:19] if isinstance(started_at, str) and len(started_at) > 19 else started_at}", style={"marginBottom": "4px"}),
        html.P(f"Current step: {current_step}", style={"fontWeight": "bold", "marginBottom": "12px"}),
        html.Table(
            [html.Thead(html.Tr([html.Th("Step", style={"padding": "4px 12px"}), html.Th("Status", style={"padding": "4px 12px"}), html.Th("Done / failed at", style={"padding": "4px 12px"})]))]
            + step_rows,
            style={"borderCollapse": "collapse", "marginBottom": "24px", "width": "100%", "maxWidth": "100%"},
        ),

        html.H2("Queue-times job", style={"marginTop": "24px", "marginBottom": "8px"}),
        html.P("Running: Yes" if queue_running else "Running: No", style={"fontWeight": "bold", "marginBottom": "8px"}),
    ]

    # Recent times collected (sample from staging)
    sample = get_recent_queue_times_sample(base, max_rows=15)
    cell_style = {"padding": "4px 8px"}
    if sample:
        # Shorten observed_at for display (e.g. "2026-01-30T21:43:00-05:00" -> "01-30 21:43")
        def short_time(s: str) -> str:
            if not s or not isinstance(s, str):
                return "—"
            try:
                # Keep date and time, drop TZ for brevity
                if "T" in s:
                    d, t = s.split("T", 1)
                    t = t[:5] if len(t) >= 5 else t  # HH:MM
                    return f"{d[-5:] if len(d) >= 10 else d} {t}"
            except Exception:
                pass
            return s[:16] if len(s) > 16 else s
        qt_header = html.Thead(html.Tr([
            html.Th("Entity", style=cell_style), html.Th("Observed at", style=cell_style), html.Th("Wait (min)", style=cell_style),
        ]))
        qt_body = html.Tbody([
            html.Tr([html.Td(r["entity"], style=cell_style), html.Td(short_time(r["observed_at"]), style=cell_style), html.Td(r["wait_min"], style=cell_style)])
            for r in sample
        ])
        layout.append(
            html.P("Recent times collected (sample):", style={"marginBottom": "4px", "fontSize": "14px"})
        )
        layout.append(
            html.Table(
                [qt_header, qt_body],
                style={"borderCollapse": "collapse", "width": "100%", "maxWidth": "100%", "fontSize": "13px", "marginBottom": "24px"},
            )
        )
    else:
        layout.append(html.P("No recent queue-times data in staging yet.", style={"marginBottom": "24px", "fontSize": "14px", "color": "#666"}))

    # Entities section: show workers and in-progress (one entity or list when parallel)
    entities_summary = f"Total: {total_entities}  |  Completed: {current_index}"
    if training_workers and training_workers > 1:
        entities_summary += f"  |  Workers: {training_workers}"
    if running_entities:
        in_progress = ", ".join(running_entities[:8]) + (" …" if len(running_entities) > 8 else "")
        entities_summary += f"  |  In progress: {in_progress}"
    elif current_entity:
        entities_summary += f"  |  In progress: {current_entity}"
    layout.extend([
        html.H2("Entities (this run)", style={"marginTop": "24px", "marginBottom": "8px"}),
        html.P(entities_summary, style={"marginBottom": "12px"}),
    ])

    # Entities table — use div with overflow to avoid horizontal scroll; table fits width
    if rows:
        table_header = html.Thead(html.Tr([
            html.Th("Entity"), html.Th("Name"), html.Th("Status"), html.Th("Rows"),
            html.Th("ACTUAL"), html.Th("POSTED"), html.Th("PRIORITY"), html.Th("Latest date"),
        ]))
        cell_style = {"padding": "4px 8px"}
        table_body = html.Tbody([
            html.Tr(
                [
                    html.Td(r["Entity"], style={**cell_style, **({"backgroundColor": "#e8f4e8"} if r["running"] else {})}),
                    html.Td(r["Name"], style={**cell_style, **({"backgroundColor": "#e8f4e8"} if r["running"] else {})}),
                    html.Td(r["Status"], style={**cell_style, **({"backgroundColor": "#e8f4e8"} if r["running"] else {})}),
                    html.Td(r["Rows"], style=cell_style),
                    html.Td(r["ACTUAL"], style=cell_style),
                    html.Td(r["POSTED"], style=cell_style),
                    html.Td(r["PRIORITY"], style=cell_style),
                    html.Td(r["Latest date"], style=cell_style),
                ],
                style={"backgroundColor": "#e8f4e8" if r["running"] else None},
            )
            for r in rows
        ])
        layout.append(
            html.Div(
                html.Table(
                    [table_header, table_body],
                    style={"borderCollapse": "collapse", "width": "100%", "fontSize": "14px"},
                ),
                style={"overflowX": "auto", "maxWidth": "100%", "marginBottom": "24px"},
            )
        )
    else:
        layout.append(html.P("No entities in this run yet.", style={"marginBottom": "24px"}))

    return layout


# App
app = dash.Dash(__name__, title="Pipeline status", update_title="Pipeline status")
app.layout = html.Div([
    dcc.Interval(id="interval", interval=5 * 60 * 1000, n_intervals=0),  # 5 minutes
    html.Div(id="content", children=build_layout()),
], style={"fontFamily": "sans-serif", "margin": "24px", "maxWidth": "100%", "boxSizing": "border-box"})


@app.callback(Output("content", "children"), Input("interval", "n_intervals"))
def refresh(_):
    return build_layout()


# Optional Basic Auth
if dash_auth and os.environ.get("DASH_USER") and os.environ.get("DASH_PASSWORD"):
    app.auth = dash_auth.BasicAuth(
        app,
        {os.environ["DASH_USER"]: os.environ["DASH_PASSWORD"]}
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
