-- Chart type: line chart, one line per series (claims / accidents / policies_issued), x = month.
-- Puts claims, accidents, and policy issuance on one monthly timeline so you
-- can see whether they move together or diverge (e.g. accidents up while
-- claims stay flat).
SELECT
    month,
    sum(claims)          AS claims,
    sum(accidents)        AS accidents,
    sum(policies_issued) AS policies_issued
FROM (
    SELECT claim_year_month AS month, number_of_claims AS claims, 0 AS accidents, 0 AS policies_issued
    FROM insurance.gold_claims_monthly
    UNION ALL
    SELECT accident_year_month AS month, 0 AS claims, number_of_accidents AS accidents, 0 AS policies_issued
    FROM insurance.gold_accidents_monthly
    UNION ALL
    SELECT year_month AS month, 0 AS claims, 0 AS accidents, policies_issued
    FROM insurance.gold_policies_monthly
)
GROUP BY month
ORDER BY month;
