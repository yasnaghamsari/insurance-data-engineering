-- Chart type: scalar / number card.
-- All-time total recorded accidents.
SELECT sum(number_of_accidents) AS total_accidents
FROM insurance.gold_accidents_monthly;
