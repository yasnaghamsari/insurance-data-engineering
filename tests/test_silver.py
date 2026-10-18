from pyspark.sql import Row

from insurance_pipeline.silver import (
    get_column_case_insensitive,
    load_schema,
    rename_from_schema,
    require_non_null,
    transform_claims,
    transform_policies,
)


def test_run_silver_writes_all_tables(spark, silver_tables):
    assert spark.table("silver_policies").count() > 0
    assert spark.table("silver_claims").count() > 0
    assert spark.table("silver_accidents").count() > 0


def test_silver_policies_columns_are_renamed(spark, silver_tables):
    columns = spark.table("silver_policies").columns
    assert "customer_id" in columns
    assert "policy_number" in columns
    assert "CUST_ID" not in columns


def test_rename_from_schema_applies_alias(spark):
    df = spark.createDataFrame([Row(CUST_ID="1", POLICY_NO="p1")])
    schema = [
        {"name": "cust_id", "alias": "customer_id"},
        {"name": "policy_no", "alias": "policy_number"},
    ]

    renamed = rename_from_schema(df, schema)

    assert set(renamed.columns) == {"customer_id", "policy_number"}


def test_get_column_case_insensitive_raises_for_missing_column(spark):
    df = spark.createDataFrame([Row(a=1)])
    try:
        get_column_case_insensitive(df, "b")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for missing column")


def test_require_non_null_drops_rows_with_nulls(spark):
    df = spark.createDataFrame([Row(id=1, name="a"), Row(id=2, name=None)])
    result = require_non_null(df, ["name"])
    assert result.count() == 1
    assert result.first()["id"] == 1


def _claim_row(**overrides):
    base = dict(
        claim_no="c1",
        policy_no="p1",
        claim_datetime="2016-11-22 15:21:00",
        incident=Row(date="06-08-2016", hour=20, type="Single Vehicle Collision", severity="Trivial Damage"),
        collision=Row(type="Side Collision", number_of_vehicles_involved=1),
        driver=Row(age=30.0, insured_relationship="own-child", license_issue_date="01-01-1900"),
        claim_amount=Row(total=100.0, injury=10.0, property=20.0, vehicle=70.0),
        number_of_witnesses=1,
        suspicious_activity=False,
        months_as_customer=12,
        ingestion_timestamp="2024-01-01 00:00:00",
    )
    base.update(overrides)
    return Row(**base)


def test_transform_claims_parses_dates_and_derives_flag(spark):
    df = spark.createDataFrame([_claim_row()])

    result = transform_claims(df).first()

    assert result["claim_number"] == "c1"
    assert str(result["incident_date"]) == "2016-08-06"
    assert result["injury_to_person"] == 1


def test_transform_claims_nulls_malformed_license_date(spark):
    df = spark.createDataFrame(
        [_claim_row(driver=Row(age=30.0, insured_relationship="own-child", license_issue_date="not-a-date"))]
    )

    result = transform_claims(df).first()

    assert result["license_issue_date"] is None


def test_transform_claims_drops_rows_missing_required_fields(spark):
    # Two rows so Spark can infer claim_no's type from the valid row — a
    # single row with claim_no=None gives it nothing to infer from.
    df = spark.createDataFrame([_claim_row(), _claim_row(claim_no=None, policy_no="p2")])

    result = transform_claims(df)

    assert result.count() == 1
    assert result.first()["claim_number"] == "c1"


def test_transform_claims_nulls_driver_age_and_license_date_sentinels(spark):
    df = spark.createDataFrame(
        [
            _claim_row(
                claim_no="zero_age",
                driver=Row(age=0.0, insured_relationship="own-child", license_issue_date="01-01-1900"),
            ),
            _claim_row(
                claim_no="too_young",
                driver=Row(age=15.0, insured_relationship="own-child", license_issue_date="06-03-2007"),
            ),
            _claim_row(
                claim_no="valid",
                driver=Row(age=16.0, insured_relationship="wife", license_issue_date="06-03-2007"),
            ),
        ]
    )

    result = {r["claim_number"]: r for r in transform_claims(df).collect()}

    assert result["zero_age"]["driver_age"] is None
    assert result["zero_age"]["license_issue_date"] is None
    assert result["too_young"]["driver_age"] is None
    assert result["valid"]["driver_age"] == 16.0
    assert str(result["valid"]["license_issue_date"]) == "2007-03-06"


def test_transform_claims_keeps_hour_23_and_drops_hour_24(spark):
    df = spark.createDataFrame(
        [
            _claim_row(
                claim_no="h23",
                incident=Row(date="06-08-2016", hour=23, type="Parked Car", severity="Trivial Damage"),
            ),
            _claim_row(
                claim_no="h24",
                incident=Row(date="06-08-2016", hour=24, type="Parked Car", severity="Trivial Damage"),
            ),
        ]
    )

    assert [r["claim_number"] for r in transform_claims(df).collect()] == ["h23"]


def _policy_row(**overrides):
    # Raw Bronze column names, as they appear in data/samples/mysql/policies.csv.
    base = dict(
        CUST_ID=1.0,
        POLICY_NO="p1",
        POLICYTYPE="COMP",
        POL_ISSUE_DATE="04-01-2015",
        POL_EFF_DATE="24-02-2015",
        POL_EXPIRY_DATE="23-02-2016",
        BODY="SALOON",
        MAKE="TOYOTA",
        MODEL="AVALON",
        MODEL_YEAR=2004.0,
        CHASSIS_NO="C1",
        USE_OF_VEHICLE="PRIVATE",
        DRV_DOB="01-01-1980",
        BOROUGH="BRONX",
        NEIGHBORHOOD="CENTRAL BRONX",
        ZIP_CODE=10453.0,
        PRODUCT="NOT CLASSIFIED",
        SUM_INSURED=14080.0,
        PREMIUM=1090.0,
        DEDUCTABLE=2000,
    )
    base.update(overrides)
    return Row(**base)


def _transform_policy_rows(spark, cfg, rows):
    df = spark.createDataFrame(rows)
    return transform_policies(df, load_schema(cfg.policies_schema_path))


def test_transform_policies_keeps_null_sum_insured_but_drops_negatives(spark, cfg):
    rows = [
        _policy_row(POLICY_NO="null_si", SUM_INSURED=None),
        _policy_row(POLICY_NO="neg_si", SUM_INSURED=-1.0),
        _policy_row(POLICY_NO="neg_premium", PREMIUM=-1320.0),
        _policy_row(POLICY_NO="valid"),
    ]

    result = {r["policy_number"]: r for r in _transform_policy_rows(spark, cfg, rows).collect()}

    assert set(result) == {"null_si", "valid"}
    assert result["null_si"]["sum_insured"] is None


def test_transform_policies_keeps_one_row_per_policy_number(spark, cfg):
    rows = [
        # Latest issue_date wins, even with a lower premium.
        _policy_row(POLICY_NO="p1", POL_ISSUE_DATE="01-01-2017", PREMIUM=100.0),
        _policy_row(POLICY_NO="p1", POL_ISSUE_DATE="01-05-2017", PREMIUM=50.0),
        # Same issue_date: latest expiry_date wins (102000969/RI in the raw feed).
        _policy_row(POLICY_NO="p2", POL_EXPIRY_DATE="10-01-2019", BODY=None),
        _policy_row(POLICY_NO="p2", POL_EXPIRY_DATE="10-02-2019", BODY="SALOON"),
        # Everything else equal: the higher premium wins (102120560).
        _policy_row(POLICY_NO="p3", PREMIUM=0.0),
        _policy_row(POLICY_NO="p3", PREMIUM=1900.0),
        # Only the body differs (102134102): broken by sorting, so the same
        # row is always kept regardless of input order.
        _policy_row(POLICY_NO="p4", BODY="SALOON"),
        _policy_row(POLICY_NO="p4", BODY="4 WD"),
    ]

    result = _transform_policy_rows(spark, cfg, rows).collect()
    by_policy = {r["policy_number"]: r for r in result}

    assert len(result) == 4
    assert str(by_policy["p1"]["issue_date"]) == "2017-05-01"
    assert str(by_policy["p2"]["expiry_date"]) == "2019-02-10"
    assert by_policy["p3"]["premium"] == 1900.0
    assert by_policy["p4"]["body"] == "4 WD"

    reversed_result = _transform_policy_rows(spark, cfg, list(reversed(rows))).collect()
    assert {r["policy_number"]: r["body"] for r in reversed_result}["p4"] == "4 WD"


def test_silver_policies_keeps_policies_with_null_sum_insured(spark, silver_tables):
    df = spark.table("silver_policies")
    assert df.filter(df.sum_insured.isNull()).count() > 0
    assert df.count() == df.select("policy_number").distinct().count()
