-- Chart type: line chart, x = accident_year_week.
-- Finer-grained (weekly) accident volume than the monthly trend.
SELECT
    accident_year_week,
    number_of_accidents,
    average_accident_hour
FROM insurance.gold_accidents_weekly
ORDER BY accident_year_week;
