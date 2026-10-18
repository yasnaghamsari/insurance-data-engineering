-- Chart type: combo/line chart. X: year_month. Bars: total_claim_amount,
-- total_premium (or just line: loss_ratio_pct + its rolling average).
-- Monthly loss ratio: claims attributed to the month of the incident,
-- written premium to the month the policy was issued. Months with claims
-- but no written premium (incidents dated before the first policy in the
-- sample was issued) have a NULL loss_ratio_pct and rolling average, so the
-- line has a gap there rather than dropping to 0.
-- 3m_rolling_avg_loss_ratio_pct is the ratio of the current + 2 previous
-- months' summed claims to their summed premium.
-- claims_outside_policy_period counts claims whose incident date falls
-- outside the policy's effective-expiry period (most of them, on the
-- synthetic sample data — see the README's data-quality notes).
SELECT
    year_month,
    number_of_claims,
    total_claim_amount,
    total_premium,
    loss_ratio_pct,
    `3m_rolling_avg_loss_ratio_pct`,
    claims_outside_policy_period
FROM insurance.gold_loss_ratio_monthly
ORDER BY year_month;
