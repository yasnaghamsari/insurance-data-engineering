-- Chart type: stacked area or stacked bar chart, x = claim_year_month.
-- How the total claim dollar amount splits across injury/property/vehicle
-- claims each month — shows whether cost growth is driven by one category.
SELECT
    claim_year_month,
    injury_claim_amount,
    property_claim_amount,
    vehicle_claim_amount
FROM insurance.gold_claims_monthly
ORDER BY claim_year_month;
