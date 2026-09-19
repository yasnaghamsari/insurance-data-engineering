-- Chart type: table (or bar chart of number_of_accidents colored by most_common_borough).
-- The borough/zip code with the most accidents each month. most_common_borough
-- and most_common_zip_code are a true mode (most frequent value that month),
-- not an alphabetical max, so this is safe to read as "the actual hotspot".
SELECT
    accident_year_month,
    most_common_borough,
    most_common_zip_code,
    number_of_accidents
FROM insurance.gold_accidents_monthly
ORDER BY accident_year_month;
