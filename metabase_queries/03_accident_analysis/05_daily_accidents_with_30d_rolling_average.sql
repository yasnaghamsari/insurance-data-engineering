-- Chart type: line chart, x = accident_date — raw daily count vs the smoothed line.
-- Day-level accident count with a 30-day rolling average overlaid, to tell
-- day-to-day noise apart from the real underlying trend.
SELECT
    accident_date,
    number_of_accidents,
    `30d_rolling_avg_number_of_accidents` AS rolling_30d_avg
FROM insurance.gold_accidents_daily
ORDER BY accident_date;
