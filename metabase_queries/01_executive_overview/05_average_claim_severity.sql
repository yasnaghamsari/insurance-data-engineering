-- Chart type: scalar / number card.
-- Average dollar amount per claim (total claim $ / number of claims) — a
-- headline severity/efficiency metric distinct from the raw claim count.
SELECT round(sum(total_claim_amount) / sum(number_of_claims), 2) AS avg_claim_amount
FROM insurance.gold_claims_monthly;
