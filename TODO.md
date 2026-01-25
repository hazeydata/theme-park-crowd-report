# TODO / Pinned Reminders

## Queue-Times: Staging + Morning Merge (DONE)

**Implemented**: The queue-times scraper writes to **`staging/queue_times/YYYY-MM/{park}_{date}.csv`** only. Fact_tables stay **static for modelling**. The **morning ETL** (S3 run) merges **yesterday's** staging into `fact_tables/clean` at the start of each run, then deletes those staged files. The scraper runs continuously (`--interval`); staging is also available for **live use** (e.g. Twitch/YouTube).

---

## Queue-Times: Unmapped Attractions

- [ ] **Develop a process to identify attractions** in the queue-times feed that **do not have a matching dimentity code** in the master list (`config/queue_times_entity_mapping.csv`). Output a reviewable list (e.g. CSV or report) to support adding new mappings.

---

*Add new items below as needed.*
