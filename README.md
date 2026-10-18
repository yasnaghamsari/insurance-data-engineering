# Insurance Data Engineering Pipeline

An end-to-end data pipeline for an auto insurance book of business: it pulls claims, accidents, and policy data from three different source systems, cleans and validates it, aggregates it into business-ready tables, and serves it through a set of Metabase dashboards. Runs entirely locally with Docker Compose.

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

This is a [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture): each layer only cleans up after the one before it, so you can always trace a number on a dashboard back to the raw record it came from. Everything is orchestrated by Airflow.

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

### How the loss ratio is calculated

Every loss-ratio number on the dashboards (overall, monthly, and per vehicle segment) uses the same basis:

- **Claims**: incurred claim amounts for claims whose policy made it into Silver. A claim whose policy was dropped (for example, for a negative premium) is left out, because that policy's premium isn't in the denominator either.
- **Premium**: *written* premium. Each policy's full premium is counted in the month it was issued. This is not earned premium (premium spread over the policy term), which the pipeline doesn't compute.
- **Monthly attribution**: a claim counts toward the month of its *incident*, not the month it was filed. A month with claims but no written premium shows no loss ratio rather than 0%.

### Data-quality notes

The sample data is partly synthetic, and a few of its quirks shape the numbers:

- About half the policies (112,696 of 227,484 raw rows) have no sum insured. They're kept, so exposure totals only cover policies that have one.
- 10,199 raw policy rows have a negative premium and are dropped in Silver. 7 policy numbers appear more than once; Silver keeps one row per policy (latest issue date, then latest expiry, then highest premium).
- In the claims, a driver age of 0 and a licence date of 01-01-1900 are placeholders for "unknown". Silver turns them into nulls, so they don't drag down the average driver age.
- Only about 29% of claim incidents fall inside their policy's effective-to-expiry period (28.5% on the sample). Those claims are still counted. `gold_loss_ratio_monthly.claims_outside_policy_period` counts the rest per month, and the Gold step logs the total.

### Rolling averages

`30d_` columns on the daily tables average over 30 calendar days, with days that have no claims or accidents counted as zero. `4w_` columns on the weekly tables cover the current week plus the 3 before it. `3m_` columns on the monthly tables cover the current month plus the 2 before it. Weeks are ISO weeks, labelled by ISO week-based year (so 2016-01-01 falls in `2015-53`).

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
- **Overall loss ratio** — incurred claims ÷ written premium, portfolio-wide (see [How the loss ratio is calculated](#how-the-loss-ratio-is-calculated))
- **Riskiest vehicle body type** — the segment with the worst loss ratio (among those with enough policies for the number to mean anything)
- **Loss ratio trend** — how the ratio has moved month to month, by incident month, with a 3-month rolling ratio
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
├── dags/                     # the Airflow DAG
├── docker/                   # Dockerfiles and seed SQL for the local stack
├── tests/                    # pytest suite, runs against a local Spark session
├── metabase_queries/         # the SQL behind every dashboard card
├── data/                     # sample source data + the schemas used to clean it
├── docker-compose.yml
└── pyproject.toml
```

## Running it

### Locally with Docker Compose (recommended)

```bash
docker compose up -d --build
docker compose exec airflow-webserver airflow dags trigger insurance_pipeline_dag
```

This brings up Airflow (http://localhost:8080, admin/admin), MySQL and MongoDB seeded with sample data, and ClickHouse. Once the DAG finishes, check the results:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM insurance.gold_claims_monthly"
```

Then connect Metabase to ClickHouse and build the dashboards from `metabase_queries/`.

### Local development, without Docker

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
pytest -q
ruff check . && black --check .
```

Needs a JDK (11 or 17) on your `PATH` for PySpark. On Windows you'll also need `winutils.exe` for Delta Lake — Docker Compose sidesteps this entirely and is the easier path.
