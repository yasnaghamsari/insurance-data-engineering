from pyspark.sql import Row

from insurance_pipeline.silver import (
    get_column_case_insensitive,
    rename_from_schema,
    require_non_null,
    transform_claims,
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
