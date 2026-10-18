-- Chart type: scalar / number card.
-- Portfolio-wide loss ratio across the whole sample period: incurred claims
-- on Silver policies / written premium of those policies (premium counted in
-- full in its issue month — not earned premium). Same basis as the monthly
-- trend and the per-segment cards on this dashboard. Under 100% means claims
-- incurred are less than premium written.
SELECT round(sum(total_claim_amount) / sum(total_premium) * 100, 2) AS overall_loss_ratio_pct
FROM insurance.gold_loss_ratio_monthly;
