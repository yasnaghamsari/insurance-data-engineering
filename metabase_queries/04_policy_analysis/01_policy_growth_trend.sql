-- display: bar
-- size: 24x8
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["policies_issued"], "graph.x_axis.title_text": "Issue month"}
-- New policies issued per month.
SELECT year_month, policies_issued
FROM insurance.gold_policies_monthly
WHERE policies_issued > 0
ORDER BY year_month;
