-- display: scalar
-- size: 12x3
-- settings: {"column_settings": {"[\"name\",\"overall_loss_ratio_pct\"]": {"suffix": "%"}}}
-- Portfolio loss ratio over the whole sample: incurred claims on Silver
-- policies / written premium of those policies (premium counted in full in
-- its issue month, not earned premium). Under 100% means claims cost less
-- than the premium written.
SELECT round(sum(total_claim_amount) / sum(total_premium) * 100, 2) AS overall_loss_ratio_pct
FROM insurance.gold_loss_ratio_monthly;
