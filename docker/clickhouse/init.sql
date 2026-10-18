-- DDL for the Gold tables Metabase reads from. Applied automatically by the
-- clickhouse-server image's docker-entrypoint-initdb.d mechanism. Rows are
-- (re)loaded by insurance_pipeline.clickhouse_loader (truncate + insert).
--
-- Rolling-average column names say what the window actually is: `30d_` is
-- 30 calendar days (days with no rows count as zero), `4w_` is the current
-- plus 3 previous weeks, `3m_` is the current plus 2 previous months.
-- total_premium is *written* premium (by issue month), not earned premium.

CREATE DATABASE IF NOT EXISTS insurance;

CREATE TABLE IF NOT EXISTS insurance.gold_claims_daily (
    claim_date Date,
    number_of_claims Int64,
    total_claim_amount Float64,
    injury_claim_amount Float64,
    property_claim_amount Float64,
    vehicle_claim_amount Float64,
    average_incident_hour Float64,
    average_driver_age Nullable(Float64),
    pct_change_number_of_claims Nullable(Float64),
    `30d_rolling_avg_total_claim_amount` Float64
) ENGINE = MergeTree ORDER BY claim_date;

CREATE TABLE IF NOT EXISTS insurance.gold_claims_weekly (
    claim_year_week String,
    number_of_claims Int64,
    total_claim_amount Float64,
    injury_claim_amount Float64,
    property_claim_amount Float64,
    vehicle_claim_amount Float64,
    average_incident_hour Float64,
    average_driver_age Nullable(Float64),
    pct_change_number_of_claims Nullable(Float64),
    `4w_rolling_avg_total_claim_amount` Float64
) ENGINE = MergeTree ORDER BY claim_year_week;

CREATE TABLE IF NOT EXISTS insurance.gold_claims_monthly (
    claim_year_month String,
    number_of_claims Int64,
    total_claim_amount Float64,
    injury_claim_amount Float64,
    property_claim_amount Float64,
    vehicle_claim_amount Float64,
    average_incident_hour Float64,
    average_driver_age Nullable(Float64),
    pct_change_number_of_claims Nullable(Float64),
    `3m_rolling_avg_total_claim_amount` Float64
) ENGINE = MergeTree ORDER BY claim_year_month;

CREATE TABLE IF NOT EXISTS insurance.gold_accidents_daily (
    accident_date Date,
    number_of_accidents Int64,
    average_accident_hour Nullable(Float64),
    most_common_borough Nullable(String),
    most_common_zip_code Nullable(Int64),
    pct_change_number_of_accidents Nullable(Float64),
    `30d_rolling_avg_number_of_accidents` Float64
) ENGINE = MergeTree ORDER BY accident_date;

CREATE TABLE IF NOT EXISTS insurance.gold_accidents_weekly (
    accident_year_week String,
    number_of_accidents Int64,
    average_accident_hour Nullable(Float64),
    most_common_borough Nullable(String),
    most_common_zip_code Nullable(Int64),
    pct_change_number_of_accidents Nullable(Float64),
    `4w_rolling_avg_number_of_accidents` Float64
) ENGINE = MergeTree ORDER BY accident_year_week;

CREATE TABLE IF NOT EXISTS insurance.gold_accidents_monthly (
    accident_year_month String,
    number_of_accidents Int64,
    average_accident_hour Nullable(Float64),
    most_common_borough Nullable(String),
    most_common_zip_code Nullable(Int64),
    pct_change_number_of_accidents Nullable(Float64),
    `3m_rolling_avg_number_of_accidents` Float64
) ENGINE = MergeTree ORDER BY accident_year_month;

CREATE TABLE IF NOT EXISTS insurance.gold_policies_monthly (
    year_month String,
    policies_issued Int64,
    exposure Nullable(Float64),
    total_premium Float64,
    avg_issue_age_of_vehicle Nullable(Float64),
    policies_expired Int64
) ENGINE = MergeTree ORDER BY year_month;

CREATE TABLE IF NOT EXISTS insurance.gold_loss_ratio_monthly (
    year_month String,
    number_of_claims Int64,
    total_claim_amount Float64,
    claims_outside_policy_period Int64,
    total_premium Float64,
    loss_ratio_pct Nullable(Float64),
    `3m_rolling_avg_loss_ratio_pct` Nullable(Float64)
) ENGINE = MergeTree ORDER BY year_month;

CREATE TABLE IF NOT EXISTS insurance.gold_vehicle_body_risk (
    vehicle_body String,
    number_of_policies Int64,
    total_premium Float64,
    exposure Nullable(Float64),
    number_of_claims Int64,
    total_claim_amount Float64,
    avg_claim_severity Nullable(Float64),
    claims_per_100_policies Float64,
    loss_ratio_pct Nullable(Float64)
) ENGINE = MergeTree ORDER BY vehicle_body;

CREATE TABLE IF NOT EXISTS insurance.gold_vehicle_usage_risk (
    vehicle_usage String,
    number_of_policies Int64,
    total_premium Float64,
    exposure Nullable(Float64),
    number_of_claims Int64,
    total_claim_amount Float64,
    avg_claim_severity Nullable(Float64),
    claims_per_100_policies Float64,
    loss_ratio_pct Nullable(Float64)
) ENGINE = MergeTree ORDER BY vehicle_usage;

-- Accidents per ~110 m grid cell (lat/lon rounded to 3 decimals), NYC area only.
CREATE TABLE IF NOT EXISTS insurance.gold_accident_grid (
    latitude Float64,
    longitude Float64,
    number_of_accidents Int64,
    number_of_persons_injured Int64
) ENGINE = MergeTree ORDER BY (latitude, longitude);

-- Full 7 x 24 matrix, weekday_number 1 = Monday, zero-filled.
CREATE TABLE IF NOT EXISTS insurance.gold_accidents_weekday_hour (
    weekday_number Int32,
    weekday String,
    accident_hour Int32,
    number_of_accidents Int64
) ENGINE = MergeTree ORDER BY (weekday_number, accident_hour);

-- borough is NULL for accidents with no recorded borough (kept, not dropped).
CREATE TABLE IF NOT EXISTS insurance.gold_accidents_borough_monthly (
    accident_year_month String,
    borough Nullable(String),
    number_of_accidents Int64
) ENGINE = MergeTree ORDER BY accident_year_month;

-- Contributions (percentage points) add up to the portfolio loss ratio - 100.
CREATE TABLE IF NOT EXISTS insurance.gold_loss_ratio_contribution (
    vehicle_body String,
    number_of_policies Int64,
    total_premium Float64,
    total_claim_amount Float64,
    contribution_pct_points Float64,
    loss_ratio_pct Nullable(Float64)
) ENGINE = MergeTree ORDER BY vehicle_body;
