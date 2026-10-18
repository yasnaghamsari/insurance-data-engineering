-- display: scalar
-- size: 8x3
-- Accidents the source recorded without a borough. They are left out of the
-- borough comparison below and counted here instead.
SELECT sum(number_of_accidents) AS accidents_with_no_borough
FROM insurance.gold_accidents_borough_monthly
WHERE borough IS NULL;
