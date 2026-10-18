-- display: line
-- size: 12x7
-- settings: {"graph.dimensions": ["claim_year_week"], "graph.metrics": ["number_of_claims"], "graph.x_axis.title_text": "ISO week"}
-- Weekly claim volume: catches short spikes the monthly view smooths away.
SELECT
    claim_year_week,
    number_of_claims
FROM insurance.gold_claims_weekly
ORDER BY claim_year_week;
