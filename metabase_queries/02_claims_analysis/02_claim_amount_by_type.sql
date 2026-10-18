-- display: area
-- size: 12x7
-- settings: {"graph.dimensions": ["claim_year_month"], "graph.metrics": ["injury_claim_amount", "property_claim_amount", "vehicle_claim_amount"], "stackable.stack_type": "stacked", "graph.x_axis.title_text": "Claim month"}
-- How each month's claim dollars split across injury, property and vehicle
-- damage, showing whether cost growth is driven by one category.
SELECT
    claim_year_month,
    injury_claim_amount,
    property_claim_amount,
    vehicle_claim_amount
FROM insurance.gold_claims_monthly
ORDER BY claim_year_month;
