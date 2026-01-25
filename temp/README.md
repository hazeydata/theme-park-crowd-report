# Temp Directory

This directory is for temporary files and for **supplemental** scripts used occasionally (e.g. to build or adjust the queue-times mapping). The main pipeline only relies on `config/queue_times_entity_mapping.csv`; the scripts here are for one-off/rare use.

## Supplemental Queue-Times Mapping Scripts

- **`build_queue_times_mapping.py`** — Generate comparison tables (entity vs queue-times) and build the mapping from MATCHED rows.
- **`compare_entity_queue_times.py`** — Compare entity table with queue-times.com data for a park.
- **`build_queue_times_matching.py`** — Build dimentity+park, queue-times master, and matched-for-review table (steps 1–4).
- **`apply_man_review_and_build_lookup.py`** — Apply `man_review.csv` to the matched table and write `config/queue_times_entity_mapping.csv`.
- **`build_lookup_from_reviewed.py`** — Build the mapping from the reviewed matched CSV only.
- **`QUEUE_TIMES_MAPPING_README.md`** — Workflow notes for the supplemental generate/build flow.

The canonical lookup is **`config/queue_times_entity_mapping.csv`**; it is consumed by `src/get_wait_times_from_queue_times.py`.

## When You Might Use This (other)

- **Testing**: Store test files during development
- **One-off processing**: Temporary files for ad-hoc tasks
- **Debugging**: Save temporary outputs to inspect

## Cleanup

This directory can be safely cleaned up at any time. All files here are temporary.

## Note

This directory is gitignored, so any files placed here will not be tracked in version control.
