-- display: scalar
-- size: 8x3
-- settings: {"column_settings": {"[\"name\",\"pct_with_no_borough\"]": {"suffix": "%"}}}
-- Share of all accidents with no borough recorded.
SELECT round(sumIf(number_of_accidents, borough IS NULL) / sum(number_of_accidents) * 100, 1) AS pct_with_no_borough
FROM insurance.gold_accidents_borough_monthly;
