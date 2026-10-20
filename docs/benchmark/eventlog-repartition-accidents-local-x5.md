## Event log `local-1792481769460`

### Job wall time per step

| step | job wall time (s) |
|---|---:|
| bronze | 74.3 |
| silver | 54.9 |
| quality | 22.4 |
| gold | 56.2 |
| post | 14.7 |

### Per action

| step \| action | wall (s) | jobs | stages | tasks | input MB | shuffle write MB |
|---|---:|---:|---:|---:|---:|---:|
| bronze &#124; write bronze_policies | 13.6 | 4 | 3 | 18 | 189 | 152 |
| bronze &#124; write bronze_claims | 2.1 | 3 | 2 | 2 | 20 | 0.0 |
| bronze &#124; write bronze_accidents | 58.6 | 4 | 3 | 18 | 268 | 1,075 |
| Delta: silver &#124; write silver_claims: Filtering files for query | 2.3 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_claims | 1.5 | 3 | 2 | 2 | 1.7 | 0.0 |
| Delta: silver &#124; write silver_policies: Filtering files for query | 0.8 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_policies | 6.9 | 4 | 3 | 34 | 47 | 124 |
| Delta: silver &#124; write silver_accidents: Filtering files for query | 1.0 | 2 | 2 | 51 | 0.1 | 0.0 |
| silver &#124; write silver_accidents | 42.5 | 3 | 2 | 17 | 206 | 0.0 |
| Delta: Delta: quality &#124; validate silver_claims: Filtering files for query: Compute snapshot for version: 0 | 1.1 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_claims: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_claims | 1.8 | 15 | 15 | 15 | 27 | 2.1 |
| Delta: Delta: quality &#124; validate silver_policies: Filtering files for query: Compute snapshot for version: 0 | 0.6 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_policies: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_policies | 3.9 | 27 | 27 | 189 | 935 | 17 |
| Delta: Delta: quality &#124; validate silver_accidents: Filtering files for query: Compute snapshot for version: 0 | 0.5 | 3 | 3 | 52 | 0.1 | 0.0 |
| Delta: quality &#124; validate silver_accidents: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_accidents | 14.2 | 17 | 17 | 114 | 4,824 | 0.0 |
| Delta: gold &#124; write gold_claims_daily: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_daily | 1.1 | 5 | 4 | 4 | 0.4 | 0.1 |
| Delta: gold &#124; write gold_claims_weekly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_weekly | 0.9 | 5 | 4 | 4 | 0.4 | 0.0 |
| Delta: gold &#124; write gold_claims_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_monthly | 0.6 | 5 | 4 | 4 | 0.4 | 0.0 |
| Delta: gold &#124; write gold_accidents_daily: Filtering files for query | 1.1 | 4 | 4 | 200 | 0.1 | 0.0 |
| gold &#124; write gold_accidents_daily | 7.6 | 12 | 11 | 89 | 62 | 38 |
| Delta: gold &#124; write gold_accidents_weekly: Filtering files for query | 0.6 | 4 | 4 | 200 | 0.1 | 0.0 |
| gold &#124; write gold_accidents_weekly | 7.1 | 12 | 11 | 80 | 62 | 12 |
| Delta: gold &#124; write gold_accidents_monthly: Filtering files for query | 0.7 | 4 | 4 | 200 | 0.1 | 0.0 |
| gold &#124; write gold_accidents_monthly | 5.1 | 12 | 11 | 73 | 62 | 4.2 |
| Delta: gold &#124; write gold_accident_grid: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accident_grid | 3.3 | 4 | 3 | 26 | 67 | 9.3 |
| Delta: gold &#124; write gold_accidents_weekday_hour: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekday_hour | 12.5 | 6 | 5 | 35 | 17 | 0.1 |
| Delta: gold &#124; write gold_accidents_borough_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_borough_monthly | 2.2 | 4 | 3 | 18 | 14 | 0.4 |
| Delta: gold &#124; write gold_policies_monthly: Filtering files for query | 0.2 | 2 | 2 | 100 | 0.0 | 0.0 |
| gold &#124; write gold_policies_monthly | 2.2 | 7 | 6 | 38 | 6.9 | 0.2 |
| Delta: gold &#124; write gold_loss_ratio_monthly: Filtering files for query | 0.7 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_monthly | 1.9 | 7 | 6 | 38 | 12 | 0.2 |
| Delta: Delta: gold &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 0.9 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: gold &#124; first(): Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; first() | 0.0 | 2 | 2 | 2 | 0.0 | 0.0 |
| Delta: gold &#124; write gold_vehicle_body_risk: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_body_risk | 1.9 | 9 | 8 | 40 | 15 | 0.2 |
| Delta: gold &#124; write gold_vehicle_usage_risk: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_usage_risk | 1.0 | 9 | 8 | 40 | 15 | 0.0 |
| Delta: gold &#124; write gold_loss_ratio_contribution: Filtering files for query | 0.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_contribution | 2.6 | 10 | 9 | 57 | 13 | 0.2 |
| post &#124; gold row counts | 10.2 | 58 | 58 | 759 | 0.0 | 0.1 |
| Delta: Delta: post &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.6 | 6 | 6 | 153 | 0.0 | 0.0 |
| Delta: post &#124; first(): Filtering files for query | 1.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| post &#124; first() | 1.4 | 8 | 8 | 8 | 0.1 | 0.0 |

### 6 slowest stages

| stage | step \| action | duration (s) | tasks | longest task (s) | input MB | input records | shuffle read MB | shuffle write MB | spill MB | GC (s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 6 | bronze &#124; write bronze_accidents | 47.6 | 1 | 47.6 | 268 | 8,060,895 | 0.0 | 1,075 | 3,739 | 0.2 |
| 25 | silver &#124; write silver_accidents | 42.2 | 16 | 42.1 | 206 | 8,060,895 | 0.0 | 0.0 | 0.0 | 44.8 |
| 107 | quality &#124; validate silver_accidents | 13.2 | 16 | 13.2 | 192 | 7,078,940 | 0.0 | 0.0 | 0.0 | 57.4 |
| 252 | gold &#124; write gold_accidents_weekday_hour | 11.8 | 16 | 0.8 | 17 | 7,078,940 | 0.0 | 0.1 | 0.0 | 0.5 |
| 251 | gold &#124; write gold_accidents_weekday_hour | 11.2 | 16 | 11.1 | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 |
| 8 | bronze &#124; write bronze_accidents | 10.5 | 16 | 10.4 | 0.0 | 0 | 1,075 | 0.0 | 0.0 | 7.8 |
