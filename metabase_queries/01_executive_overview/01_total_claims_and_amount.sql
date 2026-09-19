-- Chart type: scalar / number cards (this returns 2 numbers — make it 2 cards,
-- or use "detail" table view with both columns).
-- All-time headline totals: how many claims, how much they cost in total.
SELECT
    sum(number_of_claims)             AS total_claims,
    round(sum(total_claim_amount), 2) AS total_claim_amount
FROM insurance.gold_claims_monthly;
