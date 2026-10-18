-- display: table
-- size: 12x16
-- settings: {"table.column_formatting": [{"columns": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], "type": "range", "colors": ["#FFFFFF", "#ED6E6E"], "min_type": null, "max_type": null, "highlight_row": false}]}
-- Accidents by hour of day (rows) and weekday (columns), colored from white
-- (fewest) to red (most) so the table reads as a heatmap.
SELECT
    leftPad(toString(accident_hour), 2, '0') AS hour,
    sumIf(number_of_accidents, weekday_number = 1) AS Mon,
    sumIf(number_of_accidents, weekday_number = 2) AS Tue,
    sumIf(number_of_accidents, weekday_number = 3) AS Wed,
    sumIf(number_of_accidents, weekday_number = 4) AS Thu,
    sumIf(number_of_accidents, weekday_number = 5) AS Fri,
    sumIf(number_of_accidents, weekday_number = 6) AS Sat,
    sumIf(number_of_accidents, weekday_number = 7) AS Sun
FROM insurance.gold_accidents_weekday_hour
GROUP BY accident_hour
ORDER BY accident_hour;
