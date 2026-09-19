-- Chart type: line or area chart, x = year_month.
-- Total sum-insured (exposure) written per month — the dollar risk the book
-- is carrying, distinct from the raw policy count.
SELECT year_month, exposure
FROM insurance.gold_policies_monthly
ORDER BY year_month;
