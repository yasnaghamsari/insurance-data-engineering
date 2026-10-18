-- display: area
-- size: 12x7
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["estimated_active_policies"], "graph.x_axis.title_text": "Month"}
-- Running total of issued minus expired policies: an estimate of how many
-- policies are in force at the end of each month.
SELECT
    year_month,
    sum(policies_issued) OVER (ORDER BY year_month)
        - sum(policies_expired) OVER (ORDER BY year_month) AS estimated_active_policies
FROM insurance.gold_policies_monthly
ORDER BY year_month;
