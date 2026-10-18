-- display: combo
-- size: 12x7
-- settings: {"graph.dimensions": ["accident_year_month"], "graph.metrics": ["number_of_accidents", "pct_change_number_of_accidents"], "series_settings": {"number_of_accidents": {"display": "bar"}, "pct_change_number_of_accidents": {"display": "line", "axis": "right", "color": "#2D3A55"}}, "graph.x_axis.title_text": "Month"}
-- Monthly accident volume (bars) and month-over-month change in % (line, right axis).
SELECT
    accident_year_month,
    number_of_accidents,
    pct_change_number_of_accidents
FROM insurance.gold_accidents_monthly
ORDER BY accident_year_month;
