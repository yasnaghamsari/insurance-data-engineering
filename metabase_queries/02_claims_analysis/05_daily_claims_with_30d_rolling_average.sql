-- Chart type: line chart, x = claim_date — two series: total_claim_amount (noisy
-- daily line) and rolling_30d_avg_claim_amount (smoothed line) on top of it.
-- Day-level claim cost with a 30-day rolling average overlaid to separate
-- day-to-day noise from the real underlying trend.
SELECT
    claim_date,
    total_claim_amount,
    `30d_rolling_avg_total_claim_amount` AS rolling_30d_avg_claim_amount
FROM insurance.gold_claims_daily
ORDER BY claim_date;
