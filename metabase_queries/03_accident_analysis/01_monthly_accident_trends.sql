-- Chart type: combo chart — bar for number_of_accidents, line for pct_change_number_of_accidents.
-- Monthly accident volume and month-over-month growth rate.
SELECT
    accident_year_month,
    number_of_accidents,
    pct_change_number_of_accidents
FROM insurance.gold_accidents_monthly
ORDER BY accident_year_month;
