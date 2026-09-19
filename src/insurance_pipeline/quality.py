"""Data-quality gate between Silver and Gold, using Great Expectations.

This replaces the ad hoc ``require_non_null`` filter (which just silently
drops bad rows) with an explicit, reportable set of expectations per table.
A failed critical expectation raises :class:`DataQualityError`, which fails
the Airflow task instead of letting bad data flow silently into Gold.
"""

from __future__ import annotations

from dataclasses import dataclass

import great_expectations as gx
from pyspark.sql import DataFrame, SparkSession

from insurance_pipeline.config import PipelineConfig


class DataQualityError(RuntimeError):
    """Raised when one or more Silver tables fail their expectation suite."""


@dataclass
class TableCheckResult:
    table_name: str
    success: bool
    failed_expectations: list[str]


def validate_dataframe(
    spark: SparkSession, df: DataFrame, table_name: str, expectations: list
) -> TableCheckResult:
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(f"{table_name}_source")
    data_asset = data_source.add_dataframe_asset(name=f"{table_name}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{table_name}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name=f"{table_name}_suite")
    for expectation in expectations:
        suite.add_expectation(expectation)

    result = batch.validate(suite)

    failed = [r.expectation_config.type for r in result.results if not r.success]
    return TableCheckResult(table_name=table_name, success=result.success, failed_expectations=failed)


def silver_claims_expectations() -> list:
    xp = gx.expectations
    return [
        xp.ExpectColumnValuesToNotBeNull(column="claim_number"),
        xp.ExpectColumnValuesToNotBeNull(column="policy_number"),
        xp.ExpectColumnValuesToNotBeNull(column="incident_date"),
        xp.ExpectColumnValuesToBeBetween(column="incident_hour", min_value=0, max_value=24),
        xp.ExpectColumnValuesToBeBetween(column="total_claim_amount", min_value=0),
    ]


def silver_policies_expectations() -> list:
    xp = gx.expectations
    return [
        xp.ExpectColumnValuesToNotBeNull(column="customer_id"),
        xp.ExpectColumnValuesToNotBeNull(column="policy_number"),
        xp.ExpectColumnValuesToNotBeNull(column="issue_date"),
        xp.ExpectColumnValuesToBeBetween(column="premium", min_value=0),
        xp.ExpectColumnValuesToBeBetween(column="sum_insured", min_value=0),
    ]


def silver_accidents_expectations() -> list:
    xp = gx.expectations
    return [
        xp.ExpectColumnValuesToNotBeNull(column="collision_id"),
        xp.ExpectColumnValuesToNotBeNull(column="accident_date"),
        xp.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90),
        xp.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180),
    ]


def run_quality_checks(spark: SparkSession, cfg: PipelineConfig) -> list[TableCheckResult]:
    """Validate every Silver table. Raises :class:`DataQualityError` on any failure."""

    checks = [
        ("silver_claims", silver_claims_expectations()),
        ("silver_policies", silver_policies_expectations()),
        ("silver_accidents", silver_accidents_expectations()),
    ]

    results = [
        validate_dataframe(spark, spark.table(table_name), table_name, expectations)
        for table_name, expectations in checks
    ]

    failures = [r for r in results if not r.success]
    if failures:
        details = "; ".join(f"{r.table_name}: {r.failed_expectations}" for r in failures)
        raise DataQualityError(f"Data quality checks failed for: {details}")

    return results
