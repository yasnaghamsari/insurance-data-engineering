# Metabase Dashboard Queries

The SQL behind every card on every dashboard, one file per card, grouped into
one folder per dashboard. Paste these straight into Metabase's SQL editor to
rebuild the dashboards described in the main `README.md`.

All queries run directly against the `insurance` database in ClickHouse — no
joins back to Silver/Bronze, no external services.

## How to use these

1. In Metabase, add ClickHouse as a database (Admin → Databases → Add a
   database → ClickHouse), pointing at the same host/port/credentials as
   `docker-compose.yml`'s `clickhouse` service (defaults: host `clickhouse`
   or `localhost` if Metabase runs outside Docker, port `8123`, database
   `insurance`, user `default`, no password).
2. For each `.sql` file: **New → SQL query**, pick the ClickHouse database,
   paste the file's contents in, run it, then **Save** — pick a chart type
   that fits the comment at the top of the file (scalar / line / bar /
   table, etc.).
3. Group each folder's saved questions into a dashboard named after the
   folder: Executive Overview, Claims Analysis, Accident Analysis, Policy
   Analysis, Vehicle & Loss Ratio Risk.

## Folders

- **01_executive_overview** — the five headline numbers and the one trend
  chart worth checking daily.
- **02_claims_analysis** — claim volume, cost breakdown, and severity over
  time.
- **03_accident_analysis** — accident volume and where it's concentrated.
- **04_policy_analysis** — how the book of business is growing.
- **05_vehicle_and_loss_ratio_risk** — which vehicle segments are actually
  profitable, based on a real claims-to-policy join (see
  `src/insurance_pipeline/gold.py`).
