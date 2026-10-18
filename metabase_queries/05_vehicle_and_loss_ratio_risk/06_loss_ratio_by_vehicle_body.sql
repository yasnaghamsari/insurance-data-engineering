-- display: row
-- size: 24x8
-- settings: {"graph.dimensions": ["vehicle_body"], "graph.metrics": ["loss_ratio_pct"], "graph.show_values": true, "graph.max_categories_enabled": false}
-- The 15 highest loss ratios among vehicle bodies with at least 20 policies:
-- which body types are unprofitable to underwrite.
SELECT
    vehicle_body,
    loss_ratio_pct
FROM insurance.gold_vehicle_body_risk
WHERE number_of_policies >= 20
ORDER BY loss_ratio_pct DESC
LIMIT 15;
