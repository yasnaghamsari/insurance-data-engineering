-- Chart type: combo chart — bar for number_of_claims, line for pct_change_number_of_claims.
-- Monthly claim volume and month-over-month growth rate.
SELECT
    claim_year_month,
    number_of_claims,
    pct_change_number_of_claims
FROM insurance.gold_claims_monthly
ORDER BY claim_year_month;
