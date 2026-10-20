## Event log `local-1792480064744`

### Job wall time per step

| step | job wall time (s) |
|---|---:|
| bronze | 210.2 |
| silver | 185.2 |
| quality | 55.5 |
| gold | 53.4 |
| post | 14.4 |

### Per action

| step \| action | wall (s) | jobs | stages | tasks | input MB | shuffle write MB |
|---|---:|---:|---:|---:|---:|---:|
| bronze &#124; read policies.csv | 3.6 | 2 | 2 | 2 | 380 | 0.0 |
| bronze &#124; write bronze_policies | 19.1 | 3 | 2 | 2 | 379 | 0.0 |
| bronze &#124; read claims.json | 0.7 | 1 | 1 | 1 | 40 | 0.0 |
| bronze &#124; write bronze_claims | 2.8 | 3 | 2 | 2 | 40 | 0.0 |
| bronze &#124; read accidents.csv.gz | 46.0 | 2 | 2 | 2 | 536 | 0.0 |
| bronze &#124; write bronze_accidents | 137.9 | 3 | 2 | 2 | 536 | 0.0 |
| Delta: silver &#124; write silver_claims: Filtering files for query | 2.3 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_claims | 1.6 | 3 | 2 | 2 | 3.2 | 0.0 |
| Delta: silver &#124; write silver_policies: Filtering files for query | 0.8 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_policies | 15.6 | 4 | 3 | 34 | 67 | 227 |
| Delta: silver &#124; write silver_accidents: Filtering files for query | 0.9 | 2 | 2 | 51 | 0.0 | 0.0 |
| silver &#124; write silver_accidents | 163.9 | 3 | 2 | 17 | 335 | 0.0 |
| Delta: Delta: quality &#124; validate silver_claims: Filtering files for query: Compute snapshot for version: 0 | 1.0 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_claims: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_claims | 2.1 | 16 | 16 | 17 | 54 | 4.1 |
| Delta: Delta: quality &#124; validate silver_policies: Filtering files for query: Compute snapshot for version: 0 | 0.7 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_policies: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_policies | 6.0 | 27 | 27 | 205 | 1,838 | 34 |
| Delta: Delta: quality &#124; validate silver_accidents: Filtering files for query: Compute snapshot for version: 0 | 0.6 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: quality &#124; validate silver_accidents: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| quality &#124; validate silver_accidents | 44.8 | 17 | 17 | 114 | 7,956 | 0.0 |
| Delta: gold &#124; write gold_claims_daily: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_daily | 1.1 | 5 | 4 | 4 | 0.6 | 0.1 |
| Delta: gold &#124; write gold_claims_weekly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_weekly | 0.9 | 5 | 4 | 4 | 0.6 | 0.0 |
| Delta: gold &#124; write gold_claims_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_claims_monthly | 0.7 | 5 | 4 | 4 | 0.6 | 0.0 |
| Delta: gold &#124; write gold_accidents_daily: Filtering files for query | 1.0 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_daily | 4.8 | 12 | 11 | 81 | 107 | 13 |
| Delta: gold &#124; write gold_accidents_weekly: Filtering files for query | 0.6 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekly | 6.1 | 12 | 11 | 72 | 107 | 2.7 |
| Delta: gold &#124; write gold_accidents_monthly: Filtering files for query | 0.7 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_monthly | 4.8 | 12 | 11 | 71 | 107 | 0.8 |
| Delta: gold &#124; write gold_accident_grid: Filtering files for query | 0.2 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accident_grid | 4.2 | 4 | 3 | 19 | 68 | 2.3 |
| Delta: gold &#124; write gold_accidents_weekday_hour: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_weekday_hour | 12.0 | 6 | 5 | 35 | 29 | 0.0 |
| Delta: gold &#124; write gold_accidents_borough_monthly: Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; write gold_accidents_borough_monthly | 2.7 | 4 | 3 | 18 | 26 | 0.1 |
| Delta: gold &#124; write gold_policies_monthly: Filtering files for query | 0.2 | 2 | 2 | 100 | 0.0 | 0.0 |
| gold &#124; write gold_policies_monthly | 1.9 | 7 | 6 | 38 | 11 | 0.2 |
| Delta: gold &#124; write gold_loss_ratio_monthly: Filtering files for query | 0.7 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_monthly | 1.8 | 7 | 6 | 38 | 20 | 0.2 |
| Delta: Delta: gold &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.0 | 3 | 3 | 52 | 0.0 | 0.0 |
| Delta: gold &#124; first(): Filtering files for query | 0.1 | 1 | 1 | 50 | 0.0 | 0.0 |
| gold &#124; first() | 0.1 | 2 | 2 | 2 | 0.0 | 0.0 |
| Delta: gold &#124; write gold_vehicle_body_risk: Filtering files for query | 0.5 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_body_risk | 2.1 | 9 | 8 | 40 | 25 | 0.2 |
| Delta: gold &#124; write gold_vehicle_usage_risk: Filtering files for query | 0.5 | 3 | 3 | 150 | 0.0 | 0.0 |
| gold &#124; write gold_vehicle_usage_risk | 1.3 | 9 | 8 | 40 | 23 | 0.0 |
| Delta: gold &#124; write gold_loss_ratio_contribution: Filtering files for query | 0.4 | 4 | 4 | 200 | 0.0 | 0.0 |
| gold &#124; write gold_loss_ratio_contribution | 2.4 | 10 | 9 | 57 | 23 | 0.2 |
| post &#124; gold row counts | 10.3 | 58 | 58 | 759 | 0.0 | 0.1 |
| Delta: Delta: post &#124; first(): Filtering files for query: Compute snapshot for version: 0 | 1.4 | 6 | 6 | 153 | 0.0 | 0.0 |
| Delta: post &#124; first(): Filtering files for query | 1.4 | 4 | 4 | 200 | 0.0 | 0.0 |
| post &#124; first() | 1.3 | 8 | 8 | 8 | 0.1 | 0.0 |

### 8 slowest stages

| stage | step \| action | duration (s) | tasks | longest task (s) | input MB | input records | shuffle read MB | shuffle write MB | spill MB | GC (s) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 26 | silver &#124; write silver_accidents | 163.6 | 16 | 163.6 | 335 | 16,121,790 | 0.0 | 0.0 | 0.0 | 8.7 |
| 9 | bronze &#124; write bronze_accidents | 137.5 | 1 | 137.5 | 536 | 16,121,790 | 0.0 | 0.0 | 0.0 | 1.7 |
| 8 | bronze &#124; read accidents.csv.gz | 45.9 | 1 | 45.9 | 536 | 16,121,791 | 0.0 | 0.0 | 0.0 | 0.3 |
| 110 | quality &#124; validate silver_accidents | 39.3 | 16 | 39.3 | 310 | 14,157,880 | 0.0 | 0.0 | 0.0 | 5.0 |
| 2 | bronze &#124; write bronze_policies | 16.7 | 1 | 16.7 | 379 | 2,274,840 | 0.0 | 0.0 | 0.0 | 0.2 |
| 255 | gold &#124; write gold_accidents_weekday_hour | 11.5 | 16 | 1.2 | 29 | 14,157,880 | 0.0 | 0.0 | 0.0 | 0.0 |
| 19 | silver &#124; write silver_policies | 10.7 | 16 | 10.7 | 67 | 2,274,840 | 0.0 | 227 | 0.0 | 0.1 |
| 254 | gold &#124; write gold_accidents_weekday_hour | 10.4 | 16 | 10.4 | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.1 |
