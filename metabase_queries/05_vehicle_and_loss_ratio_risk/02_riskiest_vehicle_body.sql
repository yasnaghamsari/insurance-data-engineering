-- display: scalar
-- size: 12x3
-- The vehicle body with the worst loss ratio among bodies with at least 20
-- policies (fewer makes the ratio noise: one large claim can swing it).
SELECT vehicle_body
FROM insurance.gold_vehicle_body_risk
WHERE number_of_policies >= 20 AND loss_ratio_pct IS NOT NULL
ORDER BY loss_ratio_pct DESC
LIMIT 1;
