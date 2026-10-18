-- display: combo
-- size: 24x8
-- settings: {"graph.dimensions": ["claim_year_month"], "graph.metrics": ["number_of_claims", "pct_change_number_of_claims"], "series_settings": {"number_of_claims": {"display": "bar"}, "pct_change_number_of_claims": {"display": "line", "axis": "right", "color": "#2D3A55"}}, "graph.x_axis.title_text": "Claim month"}
-- Monthly claim volume (bars) and month-over-month growth in % (line, right axis).
SELECT
    claim_year_month,
    number_of_claims,
    pct_change_number_of_claims
FROM insurance.gold_claims_monthly
ORDER BY claim_year_month;
