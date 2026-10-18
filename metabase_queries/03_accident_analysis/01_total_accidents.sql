-- display: scalar
-- size: 8x3
-- All recorded NYC accidents in the Silver data (July 2012 - November 2019).
SELECT sum(number_of_accidents) AS total_accidents
FROM insurance.gold_accidents_borough_monthly;
