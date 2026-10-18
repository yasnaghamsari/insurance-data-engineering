-- display: bar
-- size: 12x7
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["policies_issued", "policies_expired"], "graph.x_axis.title_text": "Month"}
-- Policies issued vs expired each month: growth vs churn, side by side.
SELECT
    year_month,
    policies_issued,
    policies_expired
FROM insurance.gold_policies_monthly
ORDER BY year_month;
