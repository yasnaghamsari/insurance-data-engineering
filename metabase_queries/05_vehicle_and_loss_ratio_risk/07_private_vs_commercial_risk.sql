-- display: table
-- size: 24x4
-- Private vs commercial vehicles, the two dominant usage segments, side by
-- side on policy count, premium, claim frequency and loss ratio.
SELECT
    vehicle_usage,
    number_of_policies,
    total_premium,
    number_of_claims,
    claims_per_100_policies,
    loss_ratio_pct
FROM insurance.gold_vehicle_usage_risk
ORDER BY total_premium DESC
LIMIT 2;
