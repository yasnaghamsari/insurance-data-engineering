from pyspark.sql import Row

from insurance_pipeline.gold import (
    build_daily_accidents,
    build_daily_claims,
    build_monthly_loss_ratio,
    build_vehicle_body_risk,
)

GOLD_TABLE_NAMES = [
    "gold_claims_daily",
    "gold_claims_weekly",
    "gold_claims_monthly",
    "gold_accidents_daily",
    "gold_accidents_weekly",
    "gold_accidents_monthly",
    "gold_policies_monthly",
    "gold_loss_ratio_monthly",
    "gold_vehicle_body_risk",
    "gold_vehicle_usage_risk",
]


def test_run_gold_writes_all_tables(spark, gold_tables):
    for table_name in GOLD_TABLE_NAMES:
        assert spark.table(table_name).count() > 0, f"{table_name} should have rows"


def test_gold_claims_daily_is_actually_written(spark, gold_tables):
    # Regression test: the original notebook computed daily_claims but never
    # called saveAsTable on it, even though the README documents
    # gold_claims_daily as an output. Guard against that regressing again.
    df = spark.table("gold_claims_daily")
    assert "pct_change_number_of_claims" in df.columns
    assert "30d_rolling_avg_total_claim_amount" in df.columns


def test_build_daily_claims_aggregates_and_computes_trend_columns(spark):
    df = spark.createDataFrame(
        [
            Row(
                claim_datetime="2024-01-01 10:00:00",
                total_claim_amount=100.0,
                injury_claim_amount=10.0,
                property_claim_amount=20.0,
                vehicle_claim_amount=70.0,
                incident_hour=10,
                driver_age=30,
            ),
            Row(
                claim_datetime="2024-01-02 11:00:00",
                total_claim_amount=200.0,
                injury_claim_amount=20.0,
                property_claim_amount=40.0,
                vehicle_claim_amount=140.0,
                incident_hour=11,
                driver_age=40,
            ),
        ]
    )

    result = build_daily_claims(df).orderBy("claim_date").collect()

    assert [r["number_of_claims"] for r in result] == [1, 1]
    assert result[0]["pct_change_number_of_claims"] is None
    assert result[1]["pct_change_number_of_claims"] == 0.0


def test_build_daily_accidents_picks_most_frequent_borough_not_max(spark):
    # "Manhattan" sorts after "Bronx" alphabetically, so F.max() (the
    # original, buggy approach) would incorrectly pick it even though
    # "Bronx" is the actually most frequent borough on this date.
    df = spark.createDataFrame(
        [
            Row(accident_date="2024-01-01", accident_hour=10, borough="Bronx", zip_code=10451),
            Row(accident_date="2024-01-01", accident_hour=11, borough="Bronx", zip_code=10451),
            Row(accident_date="2024-01-01", accident_hour=12, borough="Manhattan", zip_code=10001),
        ]
    )

    result = build_daily_accidents(df).first()

    assert result["number_of_accidents"] == 3
    assert result["most_common_borough"] == "Bronx"
    assert result["most_common_zip_code"] == 10451


def test_build_monthly_loss_ratio_divides_claims_by_premium(spark):
    claims_monthly = spark.createDataFrame(
        [
            Row(claim_year_month="2024-01", total_claim_amount=100.0),
            Row(claim_year_month="2024-02", total_claim_amount=50.0),
        ]
    )
    policies_monthly = spark.createDataFrame(
        [
            Row(year_month="2024-01", total_premium=1000.0),
            # No premium written this month (e.g. issuances had already
            # tapered off) — loss_ratio_pct must be null, not a division
            # error, since there's nothing to divide by.
            Row(year_month="2024-02", total_premium=0.0),
        ]
    )

    result = {r["year_month"]: r for r in build_monthly_loss_ratio(claims_monthly, policies_monthly).collect()}

    assert result["2024-01"]["loss_ratio_pct"] == 10.0
    assert result["2024-02"]["loss_ratio_pct"] is None


def test_build_vehicle_body_risk_attributes_claims_via_policy_join(spark):
    # Claims carry no vehicle attributes of their own (body type lives on
    # the policy record), so this has to join back through policy_number
    # rather than reading a column that exists directly on either table.
    silver_policies = spark.createDataFrame(
        [
            Row(policy_number="P1", body="SUV", premium=100.0, sum_insured=1000.0),
            Row(policy_number="P2", body="SUV", premium=100.0, sum_insured=1000.0),
            Row(policy_number="P3", body="SALOON", premium=200.0, sum_insured=2000.0),
            # Blank body (present in the raw feed) must be excluded, not
            # counted as its own "" segment.
            Row(policy_number="P4", body="", premium=50.0, sum_insured=500.0),
        ]
    )
    silver_claims = spark.createDataFrame(
        [
            Row(policy_number="P1", total_claim_amount=50.0),
        ]
    )

    result = {
        r["vehicle_body"]: r for r in build_vehicle_body_risk(silver_claims, silver_policies).collect()
    }

    assert set(result.keys()) == {"SUV", "SALOON"}
    assert result["SUV"]["number_of_policies"] == 2
    assert result["SUV"]["number_of_claims"] == 1
    assert result["SUV"]["claims_per_100_policies"] == 50.0
    assert result["SUV"]["loss_ratio_pct"] == 25.0
    assert result["SALOON"]["number_of_claims"] == 0
    assert result["SALOON"]["loss_ratio_pct"] == 0.0


def test_build_daily_accidents_excludes_nulls_from_the_mode(spark):
    # On the real data, rows with a missing borough outnumber any single
    # named borough in roughly half the months. An unfiltered mode would
    # report null as "most common" there, which is technically correct but
    # useless — this asserts a real (non-null) borough wins over a more
    # frequent but null one.
    df = spark.createDataFrame(
        [
            Row(accident_date="2024-01-01", accident_hour=10, borough=None, zip_code=None),
            Row(accident_date="2024-01-01", accident_hour=11, borough=None, zip_code=None),
            Row(accident_date="2024-01-01", accident_hour=12, borough="Queens", zip_code=11101),
        ]
    )

    result = build_daily_accidents(df).first()

    assert result["most_common_borough"] == "Queens"
    assert result["most_common_zip_code"] == 11101
