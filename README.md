# Insurance Data Engineering Pipeline

An end-to-end data pipeline for an auto insurance book of business: it pulls claims, accidents, and policy data from three different source systems, cleans and validates it, aggregates it into business-ready tables, and serves it through a set of Metabase dashboards. Runs entirely locally with Docker Compose, or on Databricks.

## How it works

```
MySQL / MongoDB / S3 (raw sources)
    |
    v
Bronze   — raw data, ingested as-is
    |
    v
Silver   — cleaned, standardized, typed
    |
    v
Data Quality Gate — Great Expectations checks the data before it goes further
    |
    v
Gold     — business-ready aggregates (claims, accidents, policies, loss ratio)
    |
    v
ClickHouse  →  Metabase dashboards
```

This is a [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture): each layer only cleans up after the one before it, so you can always trace a number on a dashboard back to the raw record it came from. Everything is orchestrated by Airflow, whether it's running in Docker or on Databricks.

## Tech stack

PySpark & Delta Lake for the transforms, Great Expectations for data quality, Airflow for orchestration, ClickHouse as the analytics warehouse, and Metabase for dashboards. Tested with pytest and linted with ruff/black, checked on every push via GitHub Actions.

## The data

Three domains, each from a different source, joined together for analysis:

| Domain | Source | What it captures |
|---|---|---|
| **Policies** | MySQL | who's insured, what vehicle, what premium, when it's active |
| **Claims** | MongoDB | what was claimed, how much, by whom, why |
| **Accidents** | S3 (NYC open data) | where and when accidents happened |

## The pipeline

**Bronze** ingests each source's raw data with minimal changes — just enough to land it as a table.

**Silver** cleans it up: parsing dates, standardizing column names, dropping rows with missing required fields or values that are obviously wrong (negative premiums, GPS coordinates outside Earth's valid range, that kind of thing).

**The quality gate** runs a Great Expectations suite against every Silver table as a second check — if anything still looks wrong, the pipeline stops here instead of letting it flow into Gold.

**Gold** turns clean data into the numbers a dashboard actually needs: daily/weekly/monthly rollups of claims and accidents, policy growth and exposure, and — the one part of this pipeline that actually connects claims back to the policies that generated them — real loss ratio and per-vehicle-segment risk.

## Dashboards

Five Metabase dashboards, built from the SQL in `metabase_queries/` (one file per card — paste straight into Metabase's SQL editor).

### Executive Overview
The five numbers you'd want on your desk every morning.
- **Total claims & total claim amount** — how many claims came in and what they cost, all-time
- **Total accidents** — all-time accident count
- **Total policies issued** — size of the book of business
- **Business trends** — claims, accidents, and policies issued plotted together, so you can spot whether they're moving in sync or diverging
- **Average claim severity** — average dollar cost per claim

### Claims Analysis
A closer look at what's driving claims cost.
- **Monthly claim volume & growth** — how many claims per month, and the month-over-month change
- **Claim amount by type** — how the total cost splits across injury, property, and vehicle damage
- **Weekly claim volume** — a finer-grained view that catches short spikes the monthly chart smooths over
- **Driver age & incident hour trend** — whether the typical claimant or time-of-day is shifting over time
- **Daily claims with a 30-day rolling average** — separates real trend from day-to-day noise

### Accident Analysis
Where and how often accidents are happening.
- **Monthly & weekly accident trends** — volume over time, at two levels of granularity
- **Most accident-prone location by month** — which borough and zip code saw the most accidents each month
- **Borough hotspot ranking** — across the whole dataset, which borough has topped that list the most often
- **Daily accidents with a 30-day rolling average** — same noise-vs-trend view as claims

### Policy Analysis
How the book of business is growing.
- **Policy growth trend** — new policies issued per month
- **Exposure trend** — total sum-insured written per month (the dollar risk being carried, not just the policy count)
- **Vehicle age trend** — average age of insured vehicles at issuance, a rough underwriting-risk signal
- **Issued vs. expired** — growth against churn, side by side
- **Estimated active policies** — a running count of policies actually in force

### Vehicle & Loss Ratio Risk
The dashboard that answers "are we actually making money on this?" — built from a real join between claims and the policies they were filed against, not just independent time series.
- **Overall loss ratio** — incurred claims ÷ written premium, portfolio-wide
- **Riskiest vehicle body type** — the segment with the worst loss ratio (among those with enough policies for the number to mean anything)
- **Loss ratio trend** — how the ratio has moved month to month
- **Loss ratio by vehicle body** — which vehicle types are profitable to insure and which aren't
- **Claim frequency by vehicle body** — how often each vehicle type claims, which is a different question from how expensive each claim is
- **Private vs. commercial risk** — a direct comparison of the two biggest usage segments

## Screenshots

<table>
<tr><td><img src="images/executive_overview.png" alt="Executive Overview"></td><td><img src="images/claims_analysis.png" alt="Claims Analysis"></td></tr>
<tr><td><img src="images/accident_analysis.png" alt="Accident Analysis"></td><td><img src="images/policy_analysis.png" alt="Policy Analysis"></td></tr>
<tr><td colspan="2"><img src="images/vehicle_loss_ratio_risk.png" alt="Vehicle & Loss Ratio Risk"></td></tr>
</table>

## Project layout

```
├── src/insurance_pipeline/   # all the transform logic — Bronze, Silver, quality, Gold, ClickHouse load
├── notebooks/                # Databricks entrypoints that call into src/insurance_pipeline
├── dags/                     # the Airflow DAG
├── docker/                   # Dockerfiles and seed SQL for the local stack
├── tests/                    # pytest suite, runs against a local Spark session
├── metabase_queries/         # the SQL behind every dashboard card
├── data/                     # sample source data + the schemas used to clean it
├── docker-compose.yml
└── pyproject.toml
```

## Running it

### Locally with Docker Compose (easiest — no Databricks account needed)

```bash
docker compose up -d --build
docker compose exec airflow-webserver airflow dags trigger insurance_pipeline_dag
```

This brings up Airflow (http://localhost:8080, admin/admin), MySQL and MongoDB seeded with sample data, and ClickHouse. Once the DAG finishes, check the results:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM insurance.gold_claims_monthly"
```

Then connect Metabase to ClickHouse and build the dashboards from `metabase_queries/`.

### On Databricks

Clone the repo as a Databricks Repo, then run `notebooks/bronze.py`, `notebooks/silver.py`, and `notebooks/gold.py` in order — each installs `insurance_pipeline` from the repo checkout and calls the same functions used locally. Load the Gold tables into ClickHouse with `insurance_pipeline.clickhouse_loader.run_clickhouse_load` and point Metabase at it from there.

### Local development, without Docker

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
pytest -q
ruff check . && black --check .
```

Needs a JDK (11 or 17) on your `PATH` for PySpark. On Windows you'll also need `winutils.exe` for Delta Lake — Docker Compose sidesteps this entirely and is the easier path.

---

**Yasna Kazemghamsari** — Data Engineering | PySpark | Databricks | Data Analytics
