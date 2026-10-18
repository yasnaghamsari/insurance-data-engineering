-- display: line
-- size: 12x7
-- settings: {"graph.dimensions": ["claim_date"], "graph.metrics": ["total_claim_amount", "rolling_30d_avg_claim_amount"], "series_settings": {"total_claim_amount": {"color": "#C5DDF5"}}, "graph.x_axis.title_text": "Claim date"}
-- Day-level claim cost (light line) with its 30-calendar-day rolling average
-- (dark line; days without claims count as zero) to separate noise from trend.
SELECT
    claim_date,
    total_claim_amount,
    `30d_rolling_avg_total_claim_amount` AS rolling_30d_avg_claim_amount
FROM insurance.gold_claims_daily
ORDER BY claim_date;
