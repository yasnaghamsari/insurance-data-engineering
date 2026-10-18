-- display: line
-- size: 12x7
-- settings: {"graph.dimensions": ["accident_year_week"], "graph.metrics": ["number_of_accidents", "rolling_4w_avg"], "series_settings": {"number_of_accidents": {"color": "#F7C5C5"}}, "graph.x_axis.title_text": "ISO week"}
-- Weekly accident count (light line) with its 4-week rolling average (dark
-- line). Weekly rather than daily: the 2,616 daily rows exceed the 2,000 rows
-- Metabase renders for a native question, which would cut the chart off.
SELECT
    accident_year_week,
    number_of_accidents,
    `4w_rolling_avg_number_of_accidents` AS rolling_4w_avg
FROM insurance.gold_accidents_weekly
ORDER BY accident_year_week;
