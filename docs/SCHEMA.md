# Wait Time Fact Table Schema

Canonical definition of the fact table written to `fact_tables/clean/` and used by validation, reports, and modeling.

---

## Path and layout

- **Path**: `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv` under `output_base`
- **Grouping**: One file per **(park, park_date)**. Rows are appended when the same file is written again (e.g. multiple S3 files or queue-times runs for the same park-date).
- **Park codes**: `mk`, `ep`, `hs`, `ak`, `dl`, `ca`, `ia`, `uf`, `eu`, `uh`, `tdl`, `tds` (from `entity_code` prefix: MK, EP, HS, AK, DL, CA, IA, UF, EU, USH, TDL, TDS).

---

## Columns (exactly 4)

| Column | Type | Description |
|--------|------|-------------|
| `entity_code` | str | Attraction identifier, uppercase (e.g. `MK101`, `EP09`). Prefix maps to park. |
| `observed_at` | str | Timestamp of the observation, **ISO 8601 with timezone** (e.g. `2024-01-15T10:30:00-05:00`). |
| `wait_time_type` | str | One of `POSTED`, `ACTUAL`, `PRIORITY`. |
| `wait_time_minutes` | int | Wait time in minutes. For `PRIORITY`, `8888` = sold out. |

---

## observed_at

- **Meaning**: Wall-clock time when the wait (or priority availability) was observed.
- **Format**: ISO 8601 with offset (e.g. `-05:00`, `+09:00`). No `Z`; use explicit offset.
- **Park date**: The **operational date** for a row is derived from `observed_at` using the **6 AM rule** in the **park’s timezone**: if the local hour is &lt; 6, the park date is the **previous** calendar day; otherwise it is the same calendar day. That park date (and park from `entity_code`) determines which file the row is written to.
- **Staleness**: For **queue-times** rows, `observed_at` comes from the API’s `last_updated`; the API can return stale timestamps. The queue-times scraper logs when `observed_at` is more than 24h older than the fetch time. See [STALE_OBSERVED_AT.md](STALE_OBSERVED_AT.md).

---

## wait_time_type and wait_time_minutes

| wait_time_type | Meaning | wait_time_minutes |
|----------------|---------|-------------------|
| **POSTED** | Sign / app posted wait | 0–1000. Outlier if ≥ 300. |
| **ACTUAL** | Actual wait experienced | 0–1000. Outlier if ≥ 300. |
| **PRIORITY** | Fastpass / Lightning Lane / etc. | -100–2000, or **8888** = sold out. Outlier if &lt; -100 or &gt; 2000 and ≠ 8888. |

Validation: `scripts/validate_wait_times.py` checks these ranges and flags outliers. Invalid rows cause exit 1.

---

## Sources

| Source | wait_time_type | Pipeline |
|--------|----------------|----------|
| **S3 standby** | POSTED, ACTUAL | `get_tp_wait_time_data_from_s3.py` (5am/7am ETL) |
| **S3 fastpass/priority** | PRIORITY | same ETL |
| **queue-times.com** | POSTED only | `get_wait_times_from_queue_times.py` → `staging/queue_times/`; morning ETL merges **yesterday’s** staging into `fact_tables/clean` at run start, then deletes those staged files. |

S3 and queue-times both land in the same fact CSVs; they are deduplicated (by `entity_code`, `observed_at`, `wait_time_type`, `wait_time_minutes`) within each pipeline’s own DB before write.

---

## Related

- **Entity codes**: `dimension_tables/dimentity.csv`; queue-times mapping: `config/queue_times_entity_mapping.csv`.
- **Park date / 6am rule**: `derive_park_date()` in `get_tp_wait_time_data_from_s3.py`; `build_dimdategroupid` uses Eastern for “today” with the same 6am rule.
- **Validation**: `scripts/validate_wait_times.py`. **Report**: `scripts/report_wait_time_db.py`.
