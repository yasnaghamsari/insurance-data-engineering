-- Chart type: horizontal bar chart. X (bars): vehicle_body, sorted by
-- claims_per_100_policies descending.
-- Frequency (how often a segment claims) is a different signal from
-- severity/loss ratio (how much each claim costs) — a body type can have
-- a high claim frequency but low severity, or the reverse, and the two call
-- for different underwriting responses (pricing vs. coverage limits).
SELECT
    vehicle_body,
    number_of_policies,
    number_of_claims,
    claims_per_100_policies,
    avg_claim_severity
FROM insurance.gold_vehicle_body_risk
WHERE number_of_policies >= 20
ORDER BY claims_per_100_policies DESC;
