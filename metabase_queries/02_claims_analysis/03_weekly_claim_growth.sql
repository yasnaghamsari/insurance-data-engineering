-- Chart type: line chart, x = claim_year_week.
-- Finer-grained (weekly) claim volume than the monthly trend — useful for
-- spotting short spikes the monthly view smooths away.
SELECT
    claim_year_week,
    number_of_claims,
    pct_change_number_of_claims
FROM insurance.gold_claims_weekly
ORDER BY claim_year_week;
