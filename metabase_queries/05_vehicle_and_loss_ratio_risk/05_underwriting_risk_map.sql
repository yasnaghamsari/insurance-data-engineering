-- display: scatter
-- size: 12x9
-- settings: {"graph.dimensions": ["claims_per_100_policies", "vehicle_body"], "graph.metrics": ["avg_claim_severity"], "scatter.bubble": "total_premium", "graph.x_axis.title_text": "Claims per 100 policies (frequency)", "graph.y_axis.title_text": "Average claim ($, severity)"}
-- Frequency vs severity per vehicle body (at least 20 policies and at least
-- one claim); bubble size is written premium. Top-right is expensive on both.
SELECT
    claims_per_100_policies,
    vehicle_body,
    avg_claim_severity,
    total_premium
FROM insurance.gold_vehicle_body_risk
WHERE number_of_policies >= 20 AND number_of_claims > 0
ORDER BY total_premium DESC;
