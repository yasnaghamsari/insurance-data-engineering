-- Chart type: line/area chart, x = year_month.
-- Running total of issued minus expired policies — an estimate of book size
-- (how many policies are actively in force) at the end of each month, rather
-- than just the in-month issued/expired counts.
SELECT
    year_month,
    sum(policies_issued) OVER (ORDER BY year_month)
        - sum(policies_expired) OVER (ORDER BY year_month) AS estimated_active_policies
FROM insurance.gold_policies_monthly
ORDER BY year_month;
