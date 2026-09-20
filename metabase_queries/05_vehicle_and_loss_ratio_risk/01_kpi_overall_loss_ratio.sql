-- Chart type: scalar / number card.
-- Portfolio-wide loss ratio (incurred claims / written premium) across the
-- whole sample period. This is the headline underwriting-profitability
-- metric — under 100% means claims paid out are less than premium
-- collected; the individual monthly/segment breakdowns on this dashboard
-- explain where a ratio away from that comes from.
SELECT round(sum(total_claim_amount) / sum(total_premium) * 100, 2) AS overall_loss_ratio_pct
FROM insurance.gold_loss_ratio_monthly;
