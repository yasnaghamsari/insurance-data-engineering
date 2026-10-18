-- display: scalar
-- size: 5x3
-- settings: {"scalar.compact_primary_number": true, "column_settings": {"[\"name\",\"total_claim_amount\"]": {"prefix": "$"}}}
-- All-time incurred claim amount.
SELECT round(sum(total_claim_amount)) AS total_claim_amount
FROM insurance.gold_claims_monthly;
