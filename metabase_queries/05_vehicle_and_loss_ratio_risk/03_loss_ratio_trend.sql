-- display: line
-- size: 24x8
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["loss_ratio_pct", "3m_rolling_avg_loss_ratio_pct"], "line.missing": "none", "graph.x_axis.title_text": "Incident month", "graph.y_axis.title_text": "Loss ratio %"}
-- Monthly loss ratio (claims by incident month / written premium by issue
-- month) and its 3-month rolling ratio. Months with no written premium
-- (incidents before the first policy was issued) are NULL, so the line has a
-- gap there instead of dropping to 0.
SELECT
    year_month,
    if(total_premium > 0, loss_ratio_pct, NULL) AS loss_ratio_pct,
    if(total_premium > 0, `3m_rolling_avg_loss_ratio_pct`, NULL) AS `3m_rolling_avg_loss_ratio_pct`
FROM insurance.gold_loss_ratio_monthly
ORDER BY year_month;
