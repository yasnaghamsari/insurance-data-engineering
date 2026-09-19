from pyspark.sql import Row

from insurance_pipeline.quality import (
    run_quality_checks,
    silver_claims_expectations,
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
