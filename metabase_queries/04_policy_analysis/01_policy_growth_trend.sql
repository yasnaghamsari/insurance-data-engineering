-- Chart type: bar or line chart, x = year_month.
-- New policies issued per month.
SELECT year_month, policies_issued
FROM insurance.gold_policies_monthly
ORDER BY year_month;
