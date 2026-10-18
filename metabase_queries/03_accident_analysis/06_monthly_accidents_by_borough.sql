-- display: line
-- size: 24x8
-- settings: {"graph.dimensions": ["accident_year_month", "borough"], "graph.metrics": ["number_of_accidents"], "graph.x_axis.title_text": "Month", "graph.y_axis.title_text": "Accidents"}
-- Monthly accidents per borough. Accidents with no borough are excluded here
-- (the cards above say how many there are).
SELECT
    accident_year_month,
    borough,
    number_of_accidents
FROM insurance.gold_accidents_borough_monthly
WHERE borough IS NOT NULL
ORDER BY accident_year_month, borough;
