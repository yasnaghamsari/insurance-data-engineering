-- Chart type: line chart with two lines (different y-axes), x = claim_year_month.
-- Average claimant driver age and average incident hour, over time — checks
-- whether the claimant demographic or time-of-incident pattern is shifting.
SELECT
    claim_year_month,
    average_driver_age,
    average_incident_hour
FROM insurance.gold_claims_monthly
ORDER BY claim_year_month;
