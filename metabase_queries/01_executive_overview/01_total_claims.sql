-- display: scalar
-- size: 5x3
-- All-time number of claims.
SELECT sum(number_of_claims) AS total_claims
FROM insurance.gold_claims_monthly;
