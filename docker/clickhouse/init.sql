-- DDL for the Gold tables Metabase reads from. Applied automatically by the
-- clickhouse-server image's docker-entrypoint-initdb.d mechanism. Rows are
-- (re)loaded by insurance_pipeline.clickhouse_loader (truncate + insert).

CREATE DATABASE IF NOT EXISTS insurance;

CREATE TABLE IF NOT EXISTS insurance.gold_claims_daily (
    claim_date Date,
    number_of_claims Int64,
    total_claim_amount Float64,
    injury_claim_amount Float64,
    property_claim_amount Float64,
    vehicle_claim_amount Float64,
    average_incident_hour Float64,
    average_driver_age Float64,
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
    average_driver_age Float64,
    pct_change_number_of_claims Nullable(Float64),
    `3m_rolling_avg_total_claim_amount` Float64
) ENGINE = MergeTree ORDER BY claim_year_week;

CREATE TABLE IF NOT EXISTS insurance.gold_claims_monthly (
    claim_year_month String,
    number_of_claims Int64,
    total_claim_amount Float64,
    injury_claim_amount Float64,
    property_claim_amount Float64,
    vehicle_claim_amount Float64,
    average_incident_hour Float64,
    average_driver_age Float64,
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
    `3m_rolling_avg_number_of_accidents` Float64
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
    total_claim_amount Float64,
    total_premium Float64,
    loss_ratio_pct Nullable(Float64),
    `3m_rolling_avg_loss_ratio_pct` Nullable(Float64)
) ENGINE = MergeTree ORDER BY year_month;

CREATE TABLE IF NOT EXISTS insurance.gold_vehicle_body_risk (
    vehicle_body String,
    number_of_policies Int64,
    total_premium Float64,
    exposure Float64,
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
    exposure Float64,
    number_of_claims Int64,
    total_claim_amount Float64,
    avg_claim_severity Nullable(Float64),
    claims_per_100_policies Float64,
    loss_ratio_pct Nullable(Float64)
) ENGINE = MergeTree ORDER BY vehicle_usage;
