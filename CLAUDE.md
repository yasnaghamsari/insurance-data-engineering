# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An insurance data engineering pipeline implementing a Medallion Architecture (Bronze → Silver → Gold) with PySpark and Delta Lake, gated by a Great Expectations data-quality check, loaded into ClickHouse, and visualized in Metabase. It runs two ways from one codebase:

- **Databricks**: `notebooks/{bronze,silver,gold}.py` — thin notebooks that `%pip install -e ..` the `insurance_pipeline` package and call its `run_*` functions.
- **Local / Airflow / CI**: `src/insurance_pipeline/` is the single source of truth for all transform logic. `dags/insurance_pipeline_dag.py` orchestrates it via Docker Compose; `tests/` exercises it with a local SparkSession; the Databricks notebooks call the exact same functions.

Never duplicate transform logic into a notebook — port/extend it in `src/insurance_pipeline/` and have the notebook call in.

## Commands

```bash
# Local dev env (Python 3.11 required — pyspark/delta-spark don't support newer)
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
pytest -q                          # tests/, against a local SparkSession + real sample data
ruff check . && black --check .    # notebooks/ is excluded from ruff (Databricks-injected globals: spark, dbutils, display)

# Run one pipeline step (or all) against local Spark
python -m insurance_pipeline.run {bronze,silver,quality,gold,clickhouse,all}

# Full stack via Docker Compose (Airflow + MySQL + MongoDB + ClickHouse)
docker compose up -d --build
docker compose exec airflow-webserver airflow dags trigger insurance_pipeline_dag
```

**Windows note**: PySpark needs a JDK (11/17 — not newer) on `JAVA_HOME`, and `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` should point at the venv's `python.exe` explicitly — otherwise Spark's worker subprocess resolves `python` to the Microsoft Store app-execution-alias stub and every job hangs until a socket-accept timeout (`get_local_spark_session` in `spark_utils.py` sets these automatically, but only after the *first* JVM launch already needs a JDK). Delta Lake's package resolution additionally needs `winutils.exe` on `HADOOP_HOME` for local (non-Docker) runs on Windows — Docker Compose avoids this entirely and is the recommended local path.

**Cross-process catalog note**: `python -m insurance_pipeline.run <step>` and each Airflow task are separate OS processes, each with its own JVM. Spark's default in-memory catalog only lives for one process, so without something durable behind it, `saveAsTable` in one step and `spark.table(...)` in the next would fail with `TABLE_OR_VIEW_NOT_FOUND` even though the Delta files are sitting right there on disk. `get_local_spark_session` enables Hive support backed by a local Derby DB under `warehouse_dir`'s parent for exactly this reason — don't remove it without replacing it with some other durable catalog.

- Provision source systems (optional, only needed to regenerate the Bronze inputs from scratch, or to see how a "real" cloud deployment would be provisioned — **not used by Docker Compose**, which has its own local seed scripts under `docker/`):
  - MySQL: `setup/sql/mysql/config.sql` — AWS RDS/Fivetran-specific (grants a `fivetran` user, `mysql.rds_kill`); won't run against a plain MySQL server.
  - MongoDB: `setup/mongodb/config.sh` — targets an Atlas `mongodb+srv://` connection string.
  - Databricks source/lakehouse schemas are torn down via `setup/sql/databricks/destroy.sql`.

## Architecture

### Data flow and sources
Three domains, each with a different upstream source system:

| Domain | Source system | Sample file |
|---|---|---|
| Policies | MySQL | `data/samples/mysql/policies.csv` |
| Claims | MongoDB | `data/samples/s3/tmp/claims.json` (what Bronze actually reads) / `data/samples/mongodb/claims.json` (what Docker Compose's local Mongo seed loads) |
| Accidents | S3 | `data/samples/s3/external/accidents.csv.gz` |

`data/schemas/{mongodb,s3,sql}/*.json` define per-source column name → business-name aliasing, consumed by `silver.py`'s `rename_from_schema`/`load_schema`.

### `src/insurance_pipeline/` (the real source of truth)
- `config.py` — `PipelineConfig`, a frozen dataclass reading every path/connection setting from env vars (`DATA_ROOT`, `SCHEMA_ROOT`, `WAREHOUSE_DIR`, `CLICKHOUSE_*`), with defaults relative to the repo root. This replaced hardcoded per-user Databricks workspace paths that used to live inline in the notebooks.
- `spark_utils.py` — `get_local_spark_session(cfg)` builds a Delta-enabled local `SparkSession`. Databricks notebooks never call this; they use the runtime-injected `spark` directly. Every `run_*` function takes `spark` as a parameter rather than importing a global, which is what makes the same code work in both places.
- `bronze.py` — `ingest_raw_data()` (shared CSV/JSON ingestion helper) + `run_bronze(spark, cfg)`. Drops any columns prefixed `_`, stamps `ingestion_timestamp`, writes `bronze_{policies,claims,accidents}` Delta tables.
- `silver.py` — `load_schema`, `rename_from_schema`, `require_non_null`, `write_silver_table` helpers, plus `transform_claims`/`transform_policies`/`transform_accidents` and `run_silver(spark, cfg)`. Claims flattens nested Bronze struct columns (`claim_amount.*`, `collision.*`, `driver.*`, `incident.*`) via dotted `F.col()` access; policies/accidents use `rename_from_schema` against their schema JSON — note that function only renames, it does **not** apply the schema JSON's `"type"`/`"format"` metadata, so every date column still needs an explicit `F.to_date(col, fmt)` call in the transform (see the `date_col` loop in `transform_policies`). `accident_hour` is derived here (not in Gold — see below). Date parsing uses `F.to_date`/`F.to_timestamp` (which return null on a malformed string rather than raising, given `spark.sql.legacy.timeParserPolicy=CORRECTED` set in `spark_utils.py`) — not `try_to_date`, a Databricks-only SQL function that doesn't exist in open-source Spark. Beyond `require_non_null`, both `transform_policies` and `transform_accidents` also filter out values that are non-null but objectively invalid (negative premium/sum_insured; latitude/longitude outside valid Earth coordinates) — real garbage present in the sample data that a range-based Great Expectations check in `quality.py` would otherwise just reject the whole batch on.
- `quality.py` — `run_quality_checks(spark, cfg)`: Great Expectations suites (`silver_{claims,policies,accidents}_expectations()`) run against each Silver table via `validate_dataframe()`. Raises `DataQualityError` on any failed expectation. This is the real data-quality gate now; `require_non_null` in `silver.py` is still there too (drops bad rows before they'd even reach this check).
- `gold.py` — `build_{daily,weekly,monthly}_claims`, `build_{daily,weekly,monthly}_accidents`, `build_monthly_policies`, plus `run_gold(spark, cfg)`. Same groupBy → agg → window-based trend columns (`lag` for pct-change, `rowsBetween` for rolling averages) shape across all cadences.
- `clickhouse_loader.py` — `run_clickhouse_load(spark, cfg)`: reads each `gold_*` Delta table via `.toPandas()` and loads it into ClickHouse with `clickhouse-connect`'s `insert_df`. Table DDL lives in `docker/clickhouse/init.sql`, not in Python — this module only truncates + inserts.
- `run.py` — CLI dispatcher (`python -m insurance_pipeline.run <step>`) used both for local dev and by the Airflow DAG's `BashOperator` tasks.

### Orchestration (`dags/insurance_pipeline_dag.py`)
`ingest_bronze >> transform_silver >> data_quality_checks >> build_gold >> load_clickhouse`. Each task is a `BashOperator` invoking `$PIPELINE_PYTHON -m insurance_pipeline.run <step>` — `PIPELINE_PYTHON` points at a **separate virtualenv** (`/opt/pipeline-venv`, built in `docker/airflow/Dockerfile`) rather than Airflow's own Python environment, specifically because Great Expectations' dependency tree is large enough to risk colliding with Airflow's pinned constraints. If you add a pipeline dependency, it goes in `pyproject.toml`'s main `dependencies`, not anywhere Airflow-specific.

Every task also passes `append_env=True` alongside `env=ENV`. `BashOperator.env` **replaces** the subprocess's entire environment by default rather than merging into it — without `append_env=True`, `$PATH` and `$PIPELINE_PYTHON` (both baked into the image, not in `ENV`) would be wiped and every task would fail with exit 127 ("command not found").

### Docker Compose (`docker-compose.yml`, `docker/`)
- `/opt/airflow/warehouse` is a named volume. Docker seeds a fresh named volume from whatever already exists at that mount path *in the image*, ownership included — so `docker/airflow/Dockerfile` explicitly `mkdir`s and `chown airflow:root`s it at build time. Skip that and every Spark write into the volume fails with a permission error, because Docker would otherwise create it fresh as root-owned.
- `docker/mysql/init.sql` seeds from the sample CSV with `SET SESSION sql_mode = ''` and `LOAD DATA ... IGNORE INTO TABLE` — the raw CSV has both a numeric-truncation row and duplicate `POLICY_NO` primary keys, either of which aborts the whole load under MySQL 8's default strict mode without those.
- The `docker/airflow/Dockerfile` pip install uses a BuildKit cache mount (`--mount=type=cache,target=/root/.cache/pip`) rather than `--no-cache-dir`. The pipeline venv is rebuilt from scratch on every image build (it's not editable-mounted), so without the cache mount, any one-line change under `src/` forces a full ~300MB PySpark re-download.
- Rebuilding this image repeatedly (e.g. iterating on a fix) accumulates BuildKit cache fast — run `docker builder prune` periodically rather than letting it grow unbounded; a build cache prune on an already-full disk can itself stall and take the daemon down with it.

### Downstream
Gold Delta tables → ClickHouse (`docker/clickhouse/init.sql` for schema, `clickhouse_loader.py` for data) → Metabase. Dashboard screenshots live in `images/`, referenced from `README.md`. Metabase configuration itself isn't code in this repo.

## Fixed-while-refactoring notes (won't be in git blame otherwise)

- The original notebook-only `gold.py` computed `daily_claims` (with pct-change/rolling-avg columns) but never wrote it to `gold_claims_daily`, despite the README documenting that table. `run_gold` now writes it.
- The original notebook had two conflicting `daily_accidents` definitions from iterative editing (one with trend columns that was discarded, one without that got written). `build_daily_accidents` keeps the trend columns, matching the weekly/monthly accident aggregations — but drops that discarded version's `75_pctl_number_of_vehicles_involved` column, which referenced a `number_of_vehicles_involved` field that doesn't exist anywhere in the accidents schema (it's a claims/collision field, not an accidents one). That's almost certainly why the block was discarded in the first place.
- `rename_from_schema` only renames columns; the original code never applied the schema JSON's declared `"type"`/`"format"` for `issue_date`/`effective_date`/`expiry_date`, so `silver_policies` had those as unparsed `dd-MM-yyyy` strings. Every downstream `F.year(issue_date)` in Gold silently returned null for every row, which collapsed `gold_policies_monthly` into a single NULL-keyed row instead of one row per month. Caught by actually running the pipeline end-to-end against the full sample data, not by unit tests against small crafted rows.
