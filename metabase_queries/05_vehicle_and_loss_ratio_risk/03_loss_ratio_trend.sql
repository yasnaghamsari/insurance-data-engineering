-- Chart type: combo/line chart. X: year_month. Bars: total_claim_amount,
-- total_premium (or just line: loss_ratio_pct + its rolling average).
-- The actual loss ratio over time — replaces the old "proxy" KPI on the
-- Executive Overview dashboard with a real claims/premium calculation,
-- since gold_loss_ratio_monthly is built from real dollar totals on both
-- sides rather than an approximation.
SELECT
    year_month,
    total_claim_amount,
    total_premium,
    loss_ratio_pct,
    `3m_rolling_avg_loss_ratio_pct`
FROM insurance.gold_loss_ratio_monthly
ORDER BY year_month;
