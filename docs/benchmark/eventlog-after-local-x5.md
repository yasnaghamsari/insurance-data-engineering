## Event log `local-1792488279838`

### Job wall time per step

| step | job wall time (s) |
|---|---:|
| bronze | 111.4 |
| silver | 223.8 |
| quality | 20.4 |
| gold | 39.7 |
| post | 14.7 |

### Per action

| step \| action | wall (s) | jobs | stages | tasks | input MB | shuffle write MB |
|---|---:|---:|---:|---:|---:|---:|
| bronze &#124; write bronze_policies | 15.9 | 4 | 3 | 18 | 189 | 152 |
| bronze &#124; write bronze_claims | 2.2 | 3 | 2 | 2 | 20 | 0.0 |
| bronze &#124; write bronze_accidents | 93.4 | 3 | 2 | 2 | 268 | 0.0 |
| Delta: silver &#124; write silver_claims: Filtering files for query | 3.0 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_claims | 2.0 | 3 | 2 | 2 | 1.7 | 0.0 |
| Delta: silver &#124; write silver_policies: Filtering files for query | 1.0 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_policies | 7.5 | 4 | 3 | 34 | 47 | 124 |
| Delta: silver &#124; write silver_accidents: Filtering files for query | 0.9 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_accidents | 209.4 | 3 | 2 | 17 | 169 | 0.0 |
| quality &#124; validate silver_claims | 4.3 | 12 | 12 | 61 | 3.6 | 2.1 |
| Delta: quality &#124; validate silver_claims: Filtering files for query | 3.2 | 8 | 8 | 400 | 0.0 | 0.0 |
| Delta: Delta: quality &#124; validate silver_claims: Filtering files for query: Compute snapshot for version: 0 | 0.5 | 2 | 2 | 51 | 0.0 | 0.0 |
| quality &#124; validate silver_policies | 4.5 | 12 | 12 | 121 | 25 | 17 |
| Delta: quality &#124; validate silver_policies: Filtering files for query | 3.1 | 8 | 8 | 400 | 0.1 | 0.0 |
| Delta: Delta: quality &#124; validate silver_policies: Filtering files for query: Compute snapshot for version: 0 | 0.3 | 2 | 2 | 51 | 0.0 | 0.0 |
| quality &#124; validate silver_accidents | 2.8 | 5 | 5 | 69 | 76 | 0.0 |
| Delta: quality &#124; validate silver_accidents: Filtering files for query | 1.4 | 5 | 5 | 250 | 0.0 | 0.0 |
| Delta: Delta: quality &#124; validate silver_accidents: Filtering files for query: Compute snapshot for version: 0 | 0.3 | 2 | 2 | 51 | 0.0 | 0.0 |
| Delta: gold &#124; write gold_claims_daily: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_daily | 1.1 | 5 | 4 | 4 | 0.4 | 0.1 |
| Delta: gold &#124; write gold_claims_weekly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_weekly | 0.9 | 5 | 4 | 4 | 0.4 | 0.0 |
| Delta: gold &#124; write gold_claims_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_monthly | 0.6 | 5 | 4 | 4 | 0.4 | 0.0 |
| Delta: gold &#124; write gold_accidents_daily: Filtering files for query | 0.6 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_daily | 3.9 | 12 | 11 | 77 | 54 | 8.3 |
| Delta: gold &#124; write gold_accidents_weekly: Filtering files for query | 0.7 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekly | 5.0 | 12 | 11 | 71 | 54 | 1.7 |
| Delta: gold &#124; write gold_accidents_monthly: Filtering files for query | 0.6 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_monthly | 3.8 | 12 | 11 | 71 | 54 | 0.6 |
| Delta: gold &#124; write gold_accident_grid: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accident_grid | 4.7 | 4 | 3 | 18 | 35 | 1.5 |
| Delta: gold &#124; write gold_accidents_weekday_hour: Filtering files for query | 0.3 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekday_hour | 2.2 | 5 | 4 | 34 | 15 | 0.0 |
| Delta: gold &#124; write gold_accidents_borough_monthly: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_borough_monthly | 3.1 | 4 | 3 | 18 | 13 | 0.1 |
| Delta: gold &#124; write gold_policies_monthly: Filtering files for query | 0.2 | 2 | 2 | 100 | 0.0 | 0.0 |
| gold &#124; write gold_policies_monthly | 2.0 | 7 | 6 | 38 | 6.9 | 0.2 |
| Delta: gold &#124; write gold_loss_ratio_monthly: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_monthly | 1.8 | 7 | 6 | 38 | 12 | 0.2 |
| Delta: Delta: gold &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.0 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: gold &#124; first(): Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; first() | 0.0 | 2 | 2 | 2 | 0.0 | 0.0 |
| Delta: gold &#124; write gold_vehicle_body_risk: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_body_risk | 1.7 | 9 | 8 | 40 | 15 | 0.2 |
| Delta: gold &#124; write gold_vehicle_usage_risk: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_usage_risk | 0.9 | 9 | 8 | 40 | 15 | 0.0 |
| Delta: gold &#124; write gold_loss_ratio_contribution: Filtering files for query | 0.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_contribution | 2.3 | 10 | 9 | 57 | 13 | 0.2 |
| post &#124; gold row counts | 10.3 | 58 | 58 | 759 | 0.0 | 0.1 |
| Delta: Delta: post &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.5 | 6 | 6 | 153 | 0.0 | 0.0 |
| Delta: post &#124; first(): Filtering files for query | 1.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| post &#124; first() | 1.4 | 8 | 8 | 8 | 0.1 | 0.0 |

### 6 slowest stages

| stage | step \| action | duration (s) | tasks | longest task (s) | input MB | input records | shuffle read MB | shuffle write MB | spill MB | GC (s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 23 | silver &#124; write silver_accidents | 209.1 | 16 | 209.0 | 169 | 8,060,895 | 0.0 | 0.0 | 0.0 | 1.9 |
| 6 | bronze &#124; write bronze_accidents | 92.9 | 1 | 92.9 | 268 | 8,060,895 | 0.0 | 0.0 | 0.0 | 0.5 |
| 0 | bronze &#124; write bronze_policies | 7.9 | 1 | 7.7 | 189 | 1,137,420 | 0.0 | 152 | 0.0 | 0.1 |
| 2 | bronze &#124; write bronze_policies | 5.0 | 16 | 4.9 | 0.0 | 0 | 152 | 0.0 | 0.0 | 2.3 |
| 252 | gold &#124; write gold_accident_grid | 4.0 | 16 | 4.0 | 35 | 7,078,940 | 0.0 | 1.5 | 0.0 | 0.1 |
| 199 | gold &#124; write gold_accidents_weekly | 3.8 | 16 | 3.8 | 9.9 | 7,078,940 | 0.0 | 0.0 | 0.0 | 0.1 |
