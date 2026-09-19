-- Chart type: line chart, x = year_month.
-- Average age of the insured vehicle at the time a policy is issued, over
-- time — an underwriting-risk signal (older fleets tend to mean higher risk).
SELECT year_month, avg_issue_age_of_vehicle
FROM insurance.gold_policies_monthly
ORDER BY year_month;
