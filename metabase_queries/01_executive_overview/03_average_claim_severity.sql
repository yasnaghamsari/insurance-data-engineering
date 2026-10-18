-- display: scalar
-- size: 5x3
-- settings: {"column_settings": {"[\"name\",\"avg_claim_amount\"]": {"prefix": "$", "decimals": 0}}}
-- Average dollar amount per claim (total claim $ / number of claims).
SELECT round(sum(total_claim_amount) / sum(number_of_claims), 2) AS avg_claim_amount
FROM insurance.gold_claims_monthly;
