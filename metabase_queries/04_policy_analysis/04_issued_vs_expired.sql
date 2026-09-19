-- Chart type: grouped/stacked bar chart, x = year_month, series = policies_issued vs policies_expired.
-- Policies issued vs expired each month — growth vs churn, side by side.
SELECT
    year_month,
    policies_issued,
    policies_expired,
    policies_issued - policies_expired AS net_change
FROM insurance.gold_policies_monthly
ORDER BY year_month;
