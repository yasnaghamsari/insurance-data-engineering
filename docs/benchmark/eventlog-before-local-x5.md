## Event log `local-1792487742003`

### Job wall time per step

| step | job wall time (s) |
|---|---:|
| bronze | 159.0 |
| silver | 231.8 |
| quality | 54.5 |
| gold | 49.4 |
| post | 15.0 |

### Per action

| step \| action | wall (s) | jobs | stages | tasks | input MB | shuffle write MB |
|---|---:|---:|---:|---:|---:|---:|
| bronze &#124; read policies.csv | 2.7 | 2 | 2 | 2 | 190 | 0.0 |
| bronze &#124; write bronze_policies | 15.6 | 3 | 2 | 2 | 189 | 0.0 |
| bronze &#124; read claims.json | 0.6 | 1 | 1 | 1 | 20 | 0.0 |
| bronze &#124; write bronze_claims | 2.0 | 3 | 2 | 2 | 20 | 0.0 |
| bronze &#124; read accidents.csv.gz | 35.8 | 2 | 2 | 2 | 268 | 0.0 |
| bronze &#124; write bronze_accidents | 102.3 | 3 | 2 | 2 | 268 | 0.0 |
| Delta: silver &#124; write silver_claims: Filtering files for query | 3.0 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_claims | 1.7 | 3 | 2 | 2 | 1.7 | 0.0 |
| Delta: silver &#124; write silver_policies: Filtering files for query | 1.1 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_policies | 11.5 | 4 | 3 | 27 | 33 | 116 |
| Delta: silver &#124; write silver_accidents: Filtering files for query | 1.0 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_accidents | 213.6 | 3 | 2 | 17 | 169 | 0.0 |
| Delta: Delta: quality &#124; validate silver_claims: Filtering files for query: Compute snapshot for version: 0 | 1.2 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_claims: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_claims | 2.0 | 15 | 15 | 15 | 27 | 2.1 |
| Delta: Delta: quality &#124; validate silver_policies: Filtering files for query: Compute snapshot for version: 0 | 0.6 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_policies: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_policies | 4.6 | 27 | 27 | 189 | 935 | 17 |
| Delta: Delta: quality &#124; validate silver_accidents: Filtering files for query: Compute snapshot for version: 0 | 0.6 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_accidents: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_accidents | 45.1 | 17 | 17 | 114 | 4,240 | 0.0 |
| Delta: gold &#124; write gold_claims_daily: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_daily | 1.3 | 5 | 4 | 4 | 0.4 | 0.1 |
| Delta: gold &#124; write gold_claims_weekly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_weekly | 0.9 | 5 | 4 | 4 | 0.4 | 0.0 |
| Delta: gold &#124; write gold_claims_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_monthly | 0.7 | 5 | 4 | 4 | 0.4 | 0.0 |
| Delta: gold &#124; write gold_accidents_daily: Filtering files for query | 1.0 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_daily | 3.7 | 12 | 11 | 77 | 54 | 8.3 |
| Delta: gold &#124; write gold_accidents_weekly: Filtering files for query | 0.6 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekly | 4.9 | 12 | 11 | 71 | 54 | 1.7 |
| Delta: gold &#124; write gold_accidents_monthly: Filtering files for query | 0.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_monthly | 3.9 | 12 | 11 | 71 | 54 | 0.6 |
| Delta: gold &#124; write gold_accident_grid: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accident_grid | 4.5 | 4 | 3 | 18 | 35 | 1.5 |
| Delta: gold &#124; write gold_accidents_weekday_hour: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekday_hour | 11.3 | 5 | 4 | 34 | 15 | 0.0 |
| Delta: gold &#124; write gold_accidents_borough_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_borough_monthly | 3.2 | 4 | 3 | 18 | 13 | 0.1 |
| Delta: gold &#124; write gold_policies_monthly: Filtering files for query | 0.3 | 2 | 2 | 100 | 0.0 | 0.0 |
| gold &#124; write gold_policies_monthly | 1.9 | 7 | 6 | 38 | 6.9 | 0.2 |
| Delta: gold &#124; write gold_loss_ratio_monthly: Filtering files for query | 0.7 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_monthly | 1.9 | 7 | 6 | 38 | 12 | 0.2 |
| Delta: Delta: gold &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.0 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: gold &#124; first(): Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; first() | 0.1 | 2 | 2 | 2 | 0.0 | 0.0 |
| Delta: gold &#124; write gold_vehicle_body_risk: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_body_risk | 1.7 | 9 | 8 | 40 | 15 | 0.2 |
| Delta: gold &#124; write gold_vehicle_usage_risk: Filtering files for query | 0.4 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_usage_risk | 1.0 | 9 | 8 | 40 | 15 | 0.0 |
| Delta: gold &#124; write gold_loss_ratio_contribution: Filtering files for query | 0.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_contribution | 2.4 | 10 | 9 | 57 | 13 | 0.2 |
| post &#124; gold row counts | 10.4 | 58 | 58 | 759 | 0.0 | 0.1 |
| Delta: Delta: post &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.6 | 6 | 6 | 153 | 0.0 | 0.0 |
| Delta: post &#124; first(): Filtering files for query | 1.5 | 4 | 4 | 200 | 0.0 | 0.0 |
| post &#124; first() | 1.4 | 8 | 8 | 8 | 0.1 | 0.0 |

### 6 slowest stages

| stage | step \| action | duration (s) | tasks | longest task (s) | input MB | input records | shuffle read MB | shuffle write MB | spill MB | GC (s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 26 | silver &#124; write silver_accidents | 213.2 | 16 | 213.1 | 169 | 8,060,895 | 0.0 | 0.0 | 0.0 | 2.0 |
| 9 | bronze &#124; write bronze_accidents | 101.9 | 1 | 101.8 | 268 | 8,060,895 | 0.0 | 0.0 | 0.0 | 1.5 |
| 108 | quality &#124; validate silver_accidents | 44.0 | 16 | 44.0 | 156 | 7,078,940 | 0.0 | 0.0 | 0.0 | 0.7 |
| 8 | bronze &#124; read accidents.csv.gz | 35.7 | 1 | 35.7 | 268 | 8,060,896 | 0.0 | 0.0 | 0.0 | 0.3 |
| 2 | bronze &#124; write bronze_policies | 12.7 | 1 | 12.6 | 189 | 1,137,420 | 0.0 | 0.0 | 0.0 | 0.2 |
| 255 | gold &#124; write gold_accidents_weekday_hour | 9.7 | 16 | 9.7 | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.1 |
