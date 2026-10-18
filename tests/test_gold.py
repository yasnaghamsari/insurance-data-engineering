import datetime

from pyspark.sql import Row

from insurance_pipeline.gold import (
    build_daily_accidents,
    build_daily_claims,
    build_monthly_claims,
    build_monthly_loss_ratio,
    build_vehicle_body_risk,
    build_weekly_claims,
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
    # gold_claims_daily is a documented output with trend columns.
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
    # "Manhattan" sorts after "Bronx" alphabetically, so F.max() would
    # incorrectly pick it even though
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


def _claim(claim_datetime, total_claim_amount=100.0, **overrides):
    base = dict(
        claim_datetime=claim_datetime,
        total_claim_amount=total_claim_amount,
        injury_claim_amount=0.0,
        property_claim_amount=0.0,
        vehicle_claim_amount=total_claim_amount,
        incident_hour=10,
        driver_age=30,
    )
    base.update(overrides)
    return Row(**base)


def test_build_weekly_claims_labels_weeks_by_iso_week_based_year(spark):
    dates = [
        datetime.datetime(2015, 12, 31),  # Thu, ISO 2015-W53
        datetime.datetime(2016, 1, 1),  # Fri, still ISO 2015-W53, not "2016-53"
        datetime.datetime(2016, 1, 4),  # Mon, ISO 2016-W01
        datetime.datetime(2018, 1, 1),  # Mon, ISO 2018-W01
        datetime.datetime(2018, 12, 31),  # Mon, ISO 2019-W01, not merged into "2018-01"
        datetime.datetime(2019, 1, 1),  # Tue, ISO 2019-W01
    ]
    df = spark.createDataFrame([_claim(d) for d in dates])

    rows = build_weekly_claims(df).orderBy("claim_year_week").collect()

    assert [(r["claim_year_week"], r["number_of_claims"]) for r in rows] == [
        ("2015-53", 2),
        ("2016-01", 1),
        ("2018-01", 1),
        ("2019-01", 2),
    ]
    assert "4w_rolling_avg_total_claim_amount" in rows[0].asDict()


def test_build_daily_claims_rolling_average_spans_30_calendar_days(spark):
    df = spark.createDataFrame(
        [
            _claim(datetime.datetime(2024, 1, 1), 300.0),
            _claim(datetime.datetime(2024, 1, 2), 30.0),
            # 30 days after Jan 1: the window is Jan 2 - Jan 31, so Jan 1 is
            # out, and the 28 days with no claims count as zero.
            _claim(datetime.datetime(2024, 1, 31), 600.0),
        ]
    )

    rows = build_daily_claims(df).orderBy("claim_date").collect()

    assert [r["30d_rolling_avg_total_claim_amount"] for r in rows] == [
        300.0,  # only 1 day of history so far
        165.0,  # (300 + 30) / 2 days
        21.0,  # (30 + 600) / 30 days
    ]


def test_build_monthly_claims_rolling_average_covers_three_months(spark):
    df = spark.createDataFrame(
        [_claim(datetime.datetime(2024, month, 15), month * 100.0) for month in (1, 2, 3, 4)]
    )

    rows = build_monthly_claims(df).orderBy("claim_year_month").collect()

    # April: avg(Feb, Mar, Apr) = 300, not avg(Jan..Apr) = 250.
    assert rows[-1]["3m_rolling_avg_total_claim_amount"] == 300.0


def _policy(policy_number, issue_date, premium, effective_date=None, expiry_date=None):
    return Row(
        policy_number=policy_number,
        issue_date=issue_date,
        effective_date=effective_date or issue_date,
        expiry_date=expiry_date or issue_date.replace(year=issue_date.year + 1),
        premium=premium,
    )


def _loss_claim(policy_number, incident_date, claim_datetime, total_claim_amount):
    return Row(
        policy_number=policy_number,
        incident_date=incident_date,
        claim_datetime=claim_datetime,
        total_claim_amount=total_claim_amount,
    )


def test_build_monthly_loss_ratio_uses_incident_month_and_silver_policies_only(spark):
    d = datetime.date
    silver_policies = spark.createDataFrame(
        [
            _policy("P1", d(2024, 1, 1), 1000.0),
            _policy("P2", d(2024, 3, 1), 500.0),
        ]
    )
    silver_claims = spark.createDataFrame(
        [
            # Incident in January, filed in February: belongs to January.
            _loss_claim("P1", d(2024, 1, 20), datetime.datetime(2024, 2, 3), 100.0),
            # Incident in February, but no premium was written in February:
            # the ratio must be null, not 0 or a division error.
            _loss_claim("P1", d(2024, 2, 10), datetime.datetime(2024, 2, 11), 50.0),
            # Before P2's effective date: counted, but flagged.
            _loss_claim("P2", d(2024, 2, 20), datetime.datetime(2024, 3, 5), 25.0),
            # Policy didn't survive Silver: excluded, since its premium is
            # missing from the denominator too.
            _loss_claim("DROPPED", d(2024, 1, 5), datetime.datetime(2024, 1, 6), 9999.0),
        ]
    )

    rows = build_monthly_loss_ratio(silver_claims, silver_policies).collect()
    result = {r["year_month"]: r for r in rows}

    assert set(result) == {"2024-01", "2024-02", "2024-03"}
    assert result["2024-01"]["total_claim_amount"] == 100.0
    assert result["2024-01"]["loss_ratio_pct"] == 10.0
    assert result["2024-02"]["number_of_claims"] == 2
    assert result["2024-02"]["total_premium"] == 0.0
    assert result["2024-02"]["loss_ratio_pct"] is None
    assert result["2024-02"]["3m_rolling_avg_loss_ratio_pct"] is None
    assert result["2024-02"]["claims_outside_policy_period"] == 1
    assert result["2024-03"]["number_of_claims"] == 0
    assert result["2024-03"]["loss_ratio_pct"] == 0.0
    # Ratio of the 3-month sums: (100 + 75 + 0) / (1000 + 0 + 500).
    assert result["2024-03"]["3m_rolling_avg_loss_ratio_pct"] == 11.67


def test_gold_loss_ratio_monthly_counts_only_claims_on_silver_policies(spark, gold_tables):
    on_silver_policies = (
        spark.table("silver_claims")
        .join(spark.table("silver_policies").select("policy_number"), on="policy_number", how="inner")
        .count()
    )
    totals = spark.table("gold_loss_ratio_monthly").agg({"number_of_claims": "sum"}).first()[0]
    assert totals == on_silver_policies


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

    result = {r["vehicle_body"]: r for r in build_vehicle_body_risk(silver_claims, silver_policies).collect()}

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
