-- display: waterfall
-- size: 12x9
-- settings: {"graph.dimensions": ["vehicle_body"], "graph.metrics": ["pct_points_vs_break_even"], "waterfall.show_total": true, "waterfall.increase_color": "#ED6E6E", "waterfall.decrease_color": "#88BF4D", "waterfall.total_color": "#509EE3", "graph.y_axis.title_text": "Points vs 100% loss ratio"}
-- How many percentage points each vehicle body adds to (red) or takes off
-- (green) the portfolio loss ratio relative to break-even at 100%:
-- (body's claims - body's premium) / portfolio premium. The total bar is the
-- portfolio loss ratio minus 100. The 10 biggest movers are shown; every
-- other body is summed into "All other bodies", so the total reconciles.
WITH ranked AS (
    SELECT
        vehicle_body,
        contribution_pct_points,
        row_number() OVER (ORDER BY abs(contribution_pct_points) DESC) AS rank
    FROM insurance.gold_loss_ratio_contribution
)
SELECT
    if(rank <= 10, vehicle_body, 'All other bodies') AS vehicle_body,
    round(sum(contribution_pct_points), 2) AS pct_points_vs_break_even
FROM ranked
GROUP BY vehicle_body
ORDER BY min(rank);
