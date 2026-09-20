# Metabase Dashboard Queries

Native (SQL) queries for building the five Metabase dashboards described in
the main `README.md` — Executive Overview, Claims Analysis, Accident
Analysis, Policy Analysis, and Vehicle & Loss Ratio Risk. Each folder here is
one dashboard; each `.sql` file inside it is one card/question on that
dashboard. No two queries in this whole folder are the same query reused —
every card looks at a different angle of the data.

All queries run directly against the `insurance` database in ClickHouse (the
Gold layer Metabase is connected to) — no joins back to Silver/Bronze, no
external services.

## How to use these

1. In Metabase, add ClickHouse as a database (Admin → Databases → Add a
   database → ClickHouse), pointing at the same host/port/credentials as
   `docker-compose.yml`'s `clickhouse` service (defaults: host `clickhouse`
   or `localhost` if Metabase runs outside Docker, port `8123`, database
   `insurance`, user `default`, no password).
2. For each `.sql` file: **New → SQL query**, pick the ClickHouse database,
   paste the file's contents in, run it, then **Save** — pick a chart type
   that fits the comment at the top of the file (scalar / line / bar /
   stacked area, etc.).
3. Group the five folders' saved questions into five dashboards named to
   match: Executive Overview, Claims Analysis, Accident Analysis, Policy
   Analysis, and Vehicle & Loss Ratio Risk.

## Why some of these look different from the original dashboard

A few panels from the very first draft of this project (claim severity as a
category, a full accident-location map, driver-age-bucket breakdowns) aren't
here, because the Gold tables don't carry the underlying columns needed to
build them honestly — they're monthly/weekly/daily aggregates, not row-level
Silver data. Every query below only uses columns that actually exist in the
Gold tables (see `docker/clickhouse/init.sql` for the exact schema), so
what you see in Metabase is what the data can actually support — nothing
that requires guessing or looks plausible but is secretly wrong. If you want
row-level detail (e.g. a real accident map, or driver-age-bucket
breakdowns), that would need a new Gold table exposing that grain, not a
cleverer query against the existing monthly/weekly/daily ones.

## Vehicle & Loss Ratio Risk (05)

Unlike the other four dashboards, this one isn't built purely from
independent time-series aggregates — `gold_loss_ratio_monthly`,
`gold_vehicle_body_risk`, and `gold_vehicle_usage_risk` are new Gold tables
that join Claims and Policies (something no other Gold table does), because
loss ratio and per-vehicle-segment risk genuinely require attributing a
claim back to the policy — and vehicle attributes — it was filed against.
See the module docstring in `src/insurance_pipeline/gold.py` for how the
join is structured and why it's done at the month level for loss ratio but
row level for vehicle segments (to avoid double-counting a policy's premium
across multiple claims in the same month).
