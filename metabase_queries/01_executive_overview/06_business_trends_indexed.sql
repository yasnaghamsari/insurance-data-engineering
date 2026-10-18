-- display: line
-- size: 24x9
-- settings: {"graph.dimensions": ["month"], "graph.metrics": ["claims", "accidents", "policies_issued"], "line.missing": "none", "graph.y_axis.title_text": "Index (first common month = 100)", "graph.x_axis.title_text": "Month"}
-- Claims, accidents and policies issued per month, each indexed to 100 at the
-- first month all three series have data, so they share one scale (raw
-- accidents are ~200x claims) and you can see whether they move together.
-- Only the period where all three exist is shown; a month with no data for
-- one series is NULL (a gap), not 0.
WITH
    c AS (SELECT claim_year_month AS month, number_of_claims AS v FROM insurance.gold_claims_monthly),
    a AS (SELECT accident_year_month AS month, number_of_accidents AS v FROM insurance.gold_accidents_monthly),
    p AS (SELECT year_month AS month, policies_issued AS v FROM insurance.gold_policies_monthly WHERE policies_issued > 0),
    bounds AS (
        SELECT
            greatest((SELECT min(month) FROM c), (SELECT min(month) FROM a), (SELECT min(month) FROM p)) AS first_month,
            least((SELECT max(month) FROM c), (SELECT max(month) FROM a), (SELECT max(month) FROM p)) AS last_month
    ),
    months AS (
        SELECT DISTINCT month FROM (SELECT month FROM c UNION ALL SELECT month FROM a UNION ALL SELECT month FROM p)
        WHERE month BETWEEN (SELECT first_month FROM bounds) AND (SELECT last_month FROM bounds)
    ),
    base AS (
        SELECT
            (SELECT v FROM c WHERE month = (SELECT first_month FROM bounds)) AS c0,
            (SELECT v FROM a WHERE month = (SELECT first_month FROM bounds)) AS a0,
            (SELECT v FROM p WHERE month = (SELECT first_month FROM bounds)) AS p0
    )
SELECT
    months.month AS month,
    round(c.v / (SELECT c0 FROM base) * 100, 1) AS claims,
    round(a.v / (SELECT a0 FROM base) * 100, 1) AS accidents,
    round(p.v / (SELECT p0 FROM base) * 100, 1) AS policies_issued
FROM months
LEFT JOIN c ON c.month = months.month
LEFT JOIN a ON a.month = months.month
LEFT JOIN p ON p.month = months.month
ORDER BY month
SETTINGS join_use_nulls = 1;
