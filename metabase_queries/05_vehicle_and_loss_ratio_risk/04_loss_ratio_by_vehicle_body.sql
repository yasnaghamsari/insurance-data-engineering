-- Chart type: horizontal bar chart. X (bars): vehicle_body, sorted by
-- loss_ratio_pct descending.
-- Which vehicle body types are actually unprofitable to underwrite, not
-- just which ones generate the most premium or claims volume individually.
-- Segments under 20 policies are dropped — with too few policies a single
-- large claim can swing the ratio to a number that isn't a real signal.
SELECT
    vehicle_body,
    number_of_policies,
    total_premium,
    total_claim_amount,
    loss_ratio_pct
FROM insurance.gold_vehicle_body_risk
WHERE number_of_policies >= 20
ORDER BY loss_ratio_pct DESC;
