# Metabase Dashboard Queries

The SQL behind every card on every dashboard: one folder per dashboard, one
`.sql` file per card. `scripts/setup_metabase.py` turns them into Metabase
collections, native-SQL questions and dashboards, so nothing here has to be
pasted in by hand. All queries run directly against the `insurance` database
in ClickHouse.

## File format

The card title comes from the file name (`03_loss_ratio_trend.sql` becomes
"Loss Ratio Trend"), the dashboard name from the folder name. The header
comment sets the rest:

```sql
-- display: line                     -- Metabase visualization type
-- size: 24x8                        -- width x height on the 24-column grid
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["loss_ratio_pct"]}
-- Free-text description, shown as the card's description.
SELECT ...
```

Cards are placed in file order, left to right, wrapping when a row is full:
number cards first, then the main chart at full width, then supporting
charts. Series colors are assigned centrally in `setup_metabase.py`, so a
series has the same color on every dashboard.

Metabase renders at most 2,000 rows for a native question, so no card here
returns more than that.
