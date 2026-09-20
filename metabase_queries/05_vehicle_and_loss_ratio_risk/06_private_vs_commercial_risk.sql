-- Chart type: table or grouped bar chart. X: vehicle_usage.
-- Private vs. commercial vehicles are the two dominant, and most
-- operationally meaningful, usage segments (see the long tail of
-- small/messy categories excluded implicitly by ordering on premium) —
-- commercial vehicles are typically expected to run a different risk
-- profile than private ones, so this is the direct comparison.
SELECT
    vehicle_usage,
    number_of_policies,
    total_premium,
    exposure,
    number_of_claims,
    claims_per_100_policies,
    loss_ratio_pct
FROM insurance.gold_vehicle_usage_risk
ORDER BY total_premium DESC
LIMIT 2;
