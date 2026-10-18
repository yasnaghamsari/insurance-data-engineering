-- display: line
-- size: 12x7
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["avg_issue_age_of_vehicle"], "line.missing": "none", "graph.x_axis.title_text": "Issue month", "graph.y_axis.title_text": "Years"}
-- Average age of the insured vehicle when the policy is issued (1 decimal):
-- an underwriting-risk signal, since older vehicles tend to mean higher risk.
SELECT year_month, avg_issue_age_of_vehicle
FROM insurance.gold_policies_monthly
WHERE policies_issued > 0
ORDER BY year_month;
