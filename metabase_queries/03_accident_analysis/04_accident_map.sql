-- display: map
-- size: 12x16
-- settings: {"map.type": "pin", "map.latitude_column": "latitude", "map.longitude_column": "longitude", "map.center_latitude": 40.7128, "map.center_longitude": -73.94, "map.zoom": 10}
-- The 2,000 busiest ~110 m grid cells in NYC (coordinates rounded to 3
-- decimals in Gold): where accidents concentrate. Hover a pin for its count.
SELECT
    latitude,
    longitude,
    number_of_accidents,
    number_of_persons_injured
FROM insurance.gold_accident_grid
ORDER BY number_of_accidents DESC
LIMIT 2000;
