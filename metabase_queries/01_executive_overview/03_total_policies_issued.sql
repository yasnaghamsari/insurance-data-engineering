-- Chart type: scalar / number card.
-- All-time total policies issued (book of business size, ignoring expirations).
SELECT sum(policies_issued) AS total_policies_issued
FROM insurance.gold_policies_monthly;
