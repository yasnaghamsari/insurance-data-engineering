-- display: scalar
-- size: 5x3
-- All-time recorded NYC accidents.
SELECT sum(number_of_accidents) AS total_accidents
FROM insurance.gold_accidents_monthly;
