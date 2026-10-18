-- display: line
-- size: 12x7
-- settings: {"graph.dimensions": ["claim_year_month"], "graph.metrics": ["average_driver_age", "average_incident_hour"], "series_settings": {"average_driver_age": {"color": "#A989C5"}, "average_incident_hour": {"axis": "right", "color": "#F2A86F"}}, "line.missing": "none", "graph.x_axis.title_text": "Claim month"}
-- Average claimant driver age (left axis) and average incident hour (right
-- axis) per month: is the claimant profile or time of day shifting?
SELECT
    claim_year_month,
    average_driver_age,
    average_incident_hour
FROM insurance.gold_claims_monthly
ORDER BY claim_year_month;
