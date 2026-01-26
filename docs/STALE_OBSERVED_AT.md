# Stale `observed_at` from Queue-Times API

## Summary

`observed_at` in our wait time fact rows is taken from the queue-times.com API field **`last_updated`** (per ride). The API can return **stale** timestamps: `last_updated` may be much older than our fetch time (e.g. a row staged on 2026-01-25 with `observed_at = 2026-01-20T17:56:35-05:00`). This is a **data-quality/trust** consideration, not a bug in our parsing.

## Source: `last_updated` in the API

- **Endpoint**: `https://queue-times.com/parks/{id}/queue_times.json`
- **Location**: Each ride in `lands[].rides` or top-level `rides[]` has:
  - `id`, `name`, `is_open`, `wait_time`, **`last_updated`**
- **Format**: `last_updated` is ISO 8601 (e.g. `2026-01-20T17:56:35-05:00` or `2026-01-20T22:56:35Z`). We do not control queue-times’ caching or when they set this value.

## Our Pipeline: `last_updated` → `observed_at`

1. **Read**: `last_updated = ride.get("last_updated")` in `transform_queue_times_data` ([get_wait_times_from_queue_times.py](../src/get_wait_times_from_queue_times.py)).
2. **Parse**: `pd.to_datetime(last_updated, utc=True)` → `observed_at_utc`. Pandas handles both offset (e.g. `-05:00`) and `Z`; result is timezone-aware UTC.
3. **Convert**: `observed_at_utc.tz_convert(park_tz)` → `observed_at_local`; then `observed_at_local.isoformat()` → `observed_at_str`.
4. **Use**: `observed_at_str` is written as the `observed_at` column in staging (and later in fact_tables).

We use the API value as-is; we do not replace it with fetch time. If the API returns an old `last_updated`, our `observed_at` will be old.

## Audit and Logging

- **Threshold**: `STALE_OBSERVED_AT_THRESHOLD_HOURS = 24` (in `get_wait_times_from_queue_times.py`).
- **Logic**: For each ride we compute `age_hours = (fetch_time_utc - observed_at_utc)` in hours. If `age_hours > 24`, the row is added to a stale list.
- **Logging**: When the list is non‑empty, we log a **warning** with the count and up to **3 sample** rows: `(entity_code, observed_at, age_hours, wait_time_minutes)`. This serves as a small audit to spot patterns (e.g. one park or entity often stale).

## Interpretation

- **Correct**: The API really sent an old `last_updated`; queue-times may cache or delay updates per ride.
- **Not a parsing bug**: Our `pd.to_datetime(..., utc=True)` and `tz_convert` are correct. We do not mis‑read or mis‑map the field.
- **Optional hardening**: If you need to avoid very old `observed_at` in downstream models, you could add a filter (e.g. drop rows with `age_hours > N`) or cap `observed_at`; that would be a separate, explicit design choice.

## See Also

- [get_wait_times_from_queue_times.py](../src/get_wait_times_from_queue_times.py): `transform_queue_times_data`, `STALE_OBSERVED_AT_THRESHOLD_HOURS`, and the “OBSERVED_AT” section in the module docstring.
- [docs/SCHEMA.md](SCHEMA.md): fact table columns and `observed_at` semantics.
