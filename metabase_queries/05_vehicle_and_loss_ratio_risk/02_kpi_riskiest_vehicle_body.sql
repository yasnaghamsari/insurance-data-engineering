-- Chart type: scalar / number card (or "detail" card showing the string).
-- The single vehicle body type with the worst loss ratio among segments
-- with enough policies to be meaningful (fewer than 20 policies makes the
-- ratio noisy — a single large claim can swing it wildly).
SELECT vehicle_body
FROM insurance.gold_vehicle_body_risk
WHERE number_of_policies >= 20 AND loss_ratio_pct IS NOT NULL
ORDER BY loss_ratio_pct DESC
LIMIT 1;
