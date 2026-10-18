-- display: line
-- size: 12x7
-- settings: {"graph.dimensions": ["year_month"], "graph.metrics": ["exposure"], "line.missing": "none", "graph.x_axis.title_text": "Issue month"}
-- Total sum insured (exposure) written per month. About half the policies
-- have no recorded sum insured; they count in the policy totals and premium
-- but add nothing here.
SELECT year_month, exposure
FROM insurance.gold_policies_monthly
WHERE policies_issued > 0
ORDER BY year_month;
