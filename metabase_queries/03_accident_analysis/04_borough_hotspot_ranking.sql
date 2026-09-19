-- Chart type: bar chart, x = most_common_borough, y = months_as_hotspot.
-- Across the whole dataset, which borough has been "the most accident-prone
-- borough of the month" most often — a ranking rather than a month-by-month
-- table (distinct angle from 03_most_common_location_by_month.sql).
SELECT
    most_common_borough,
    count(*) AS months_as_hotspot
FROM insurance.gold_accidents_monthly
WHERE most_common_borough IS NOT NULL
GROUP BY most_common_borough
ORDER BY months_as_hotspot DESC;
