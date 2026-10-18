from pyspark.sql import Row

from insurance_pipeline.quality import (
    run_quality_checks,
    silver_claims_expectations,
    silver_policies_expectations,
    validate_dataframe,
)


def test_run_quality_checks_passes_on_clean_silver_tables(spark, silver_tables, cfg):
    # Should not raise: the Silver layer's own require_non_null filters
    # already guarantee these tables satisfy the expectation suites.
    results = run_quality_checks(spark, cfg)
    assert all(r.success for r in results)


def test_validate_dataframe_flags_out_of_range_and_null_values(spark):
    # Two rows so Spark can infer claim_number's type from the good row — a
    # single row with claim_number=None gives it nothing to infer from.
    bad_df = spark.createDataFrame(
        [
            Row(
                claim_number="c1",
                policy_number="p1",
                incident_date="2024-01-01",
                incident_hour=10,
                total_claim_amount=5.0,
            ),
            Row(
                claim_number=None,
                policy_number="p1",
                incident_date="2024-01-01",
                incident_hour=30,
                total_claim_amount=-5.0,
            ),
        ]
    )

    result = validate_dataframe(spark, bad_df, "silver_claims_bad", silver_claims_expectations())

    assert result.success is False
    assert "expect_column_values_to_not_be_null" in result.failed_expectations
    assert "expect_column_values_to_be_between" in result.failed_expectations


def test_validate_dataframe_passes_on_good_data(spark):
    good_df = spark.createDataFrame(
        [
            Row(
                claim_number="c1",
                policy_number="p1",
                incident_date="2024-01-01",
                incident_hour=10,
                total_claim_amount=5.0,
            )
        ]
    )

    result = validate_dataframe(spark, good_df, "silver_claims_good", silver_claims_expectations())

    assert result.success is True
    assert result.failed_expectations == []


def _good_claim(**overrides):
    base = dict(
        claim_number="c1",
        policy_number="p1",
        incident_date="2024-01-01",
        incident_hour=10,
        total_claim_amount=5.0,
    )
    base.update(overrides)
    return Row(**base)


def test_claims_suite_rejects_incident_hour_24(spark):
    df = spark.createDataFrame(
        [_good_claim(incident_hour=23), _good_claim(claim_number="c2", incident_hour=24)]
    )

    result = validate_dataframe(spark, df, "silver_claims_hour", silver_claims_expectations())

    assert result.failed_expectations == ["expect_column_values_to_be_between"]


def test_claims_suite_rejects_duplicate_claim_number(spark):
    df = spark.createDataFrame([_good_claim(), _good_claim()])

    result = validate_dataframe(spark, df, "silver_claims_dupes", silver_claims_expectations())

    assert result.failed_expectations == ["expect_column_values_to_be_unique"]


def test_policies_suite_rejects_duplicate_policy_number_but_allows_null_sum_insured(spark):
    def policy(**overrides):
        base = dict(
            customer_id=1.0, policy_number="p1", issue_date="2024-01-01", premium=10.0, sum_insured=None
        )
        base.update(overrides)
        return Row(**base)

    unique_df = spark.createDataFrame([policy(), policy(policy_number="p2", sum_insured=100.0)])
    dupes_df = spark.createDataFrame([policy(sum_insured=100.0), policy(sum_insured=100.0)])

    expectations = silver_policies_expectations()
    unique_result = validate_dataframe(spark, unique_df, "silver_policies_ok", expectations)
    dupes_result = validate_dataframe(spark, dupes_df, "silver_policies_dupes", expectations)

    assert unique_result.success is True
    assert dupes_result.failed_expectations == ["expect_column_values_to_be_unique"]
