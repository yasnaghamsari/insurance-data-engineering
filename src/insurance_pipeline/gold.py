"""Gold layer: business-ready aggregations.

Ported from ``notebooks/gold.py`` and cleaned up in the process (the original
notebook had grown some copy/paste debris from iterative editing):

- ``gold_claims_daily`` is now actually written. The original notebook computed
  ``daily_claims`` (with pct-change/rolling-avg columns) but never called
  ``saveAsTable`` on it, even though the README documents the table as existing.
- ``accident_hour`` is now derived once in the Silver layer
  (:func:`insurance_pipeline.silver.transform_accidents`) instead of being
  recomputed ad hoc partway through this notebook.
- The daily accident aggregation had two conflicting definitions in the
  original notebook (one with trend columns that was immediately discarded,
  one without that actually got written). This keeps the trend columns, to
  match the weekly/monthly accident aggregations, which both already had them
  — but drops that discarded version's ``75_pctl_number_of_vehicles_involved``
  column, which referenced a ``number_of_vehicles_involved`` field that
  doesn't exist anywhere in the accidents schema (it's a claims/collision
  field); that's almost certainly *why* the block got discarded originally.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from insurance_pipeline.config import PipelineConfig


def _year_month(date_col: str) -> F.Column:
    return F.concat_ws("-", F.year(date_col), F.lpad(F.month(date_col), 2, "0"))


def _year_week(date_col: str) -> F.Column:
    return F.concat_ws("-", F.year(date_col), F.lpad(F.weekofyear(date_col), 2, "0"))


def _pct_change(count_col: str, window: Window) -> F.Column:
    current = F.col(count_col)
    previous = F.lag(count_col).over(window)
    return F.round(((current - previous) / previous) * 100, 2)


# --- Claims ---------------------------------------------------------------


def build_daily_claims(silver_claims: DataFrame) -> DataFrame:
    daily = (
        silver_claims.withColumn("claim_date", F.to_date("claim_datetime"))
        .groupBy("claim_date")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum("injury_claim_amount").alias("injury_claim_amount"),
            F.sum("property_claim_amount").alias("property_claim_amount"),
            F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
            F.round(F.avg("incident_hour")).alias("average_incident_hour"),
            F.round(F.avg("driver_age")).alias("average_driver_age"),
        )
    )

    w = Window.orderBy("claim_date")
    return daily.withColumn("pct_change_number_of_claims", _pct_change("number_of_claims", w)).withColumn(
        "30d_rolling_avg_total_claim_amount",
        F.round(F.avg("total_claim_amount").over(w.rowsBetween(-30, 0))),
    )


def build_weekly_claims(silver_claims: DataFrame) -> DataFrame:
    weekly = (
        silver_claims.withColumn("claim_year_week", _year_week("claim_datetime"))
        .groupBy("claim_year_week")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum("injury_claim_amount").alias("injury_claim_amount"),
            F.sum("property_claim_amount").alias("property_claim_amount"),
            F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
            F.round(F.avg("incident_hour")).alias("average_incident_hour"),
            F.round(F.avg("driver_age")).alias("average_driver_age"),
        )
    )

    w = Window.orderBy("claim_year_week")
    return weekly.withColumn("pct_change_number_of_claims", _pct_change("number_of_claims", w)).withColumn(
        "3m_rolling_avg_total_claim_amount",
        F.round(F.avg("total_claim_amount").over(w.rowsBetween(-3, 0))),
    )


def build_monthly_claims(silver_claims: DataFrame) -> DataFrame:
    monthly = (
        silver_claims.withColumn("claim_year_month", _year_month("claim_datetime"))
        .groupBy("claim_year_month")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum("injury_claim_amount").alias("injury_claim_amount"),
            F.sum("property_claim_amount").alias("property_claim_amount"),
            F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
            F.round(F.avg("incident_hour")).alias("average_incident_hour"),
            F.round(F.avg("driver_age")).alias("average_driver_age"),
        )
    )

    w = Window.orderBy("claim_year_month")
    return monthly.withColumn("pct_change_number_of_claims", _pct_change("number_of_claims", w)).withColumn(
        "3m_rolling_avg_total_claim_amount",
        F.round(F.avg("total_claim_amount").over(w.rowsBetween(-3, 0))),
    )


# --- Accidents --------------------------------------------------------------


def build_daily_accidents(silver_accidents: DataFrame) -> DataFrame:
    daily = silver_accidents.groupBy("accident_date").agg(
        F.count("*").alias("number_of_accidents"),
        F.round(F.avg("accident_hour")).alias("average_accident_hour"),
        F.max("borough").alias("most_common_borough"),
        F.max("zip_code").alias("most_common_zip_code"),
    )

    w = Window.orderBy("accident_date")
    return daily.withColumn(
        "pct_change_number_of_accidents", _pct_change("number_of_accidents", w)
    ).withColumn(
        "30d_rolling_avg_number_of_accidents",
        F.round(F.avg("number_of_accidents").over(w.rowsBetween(-30, 0))),
    )


def build_weekly_accidents(silver_accidents: DataFrame) -> DataFrame:
    weekly = (
        silver_accidents.withColumn("accident_year_week", _year_week("accident_date"))
        .groupBy("accident_year_week")
        .agg(
            F.count("*").alias("number_of_accidents"),
            F.round(F.avg("accident_hour")).alias("average_accident_hour"),
            F.max("borough").alias("most_common_borough"),
            F.max("zip_code").alias("most_common_zip_code"),
        )
    )

    w = Window.orderBy("accident_year_week")
    return weekly.withColumn(
        "pct_change_number_of_accidents", _pct_change("number_of_accidents", w)
    ).withColumn(
        "3m_rolling_avg_number_of_accidents",
        F.round(F.avg("number_of_accidents").over(w.rowsBetween(-3, 0))),
    )


def build_monthly_accidents(silver_accidents: DataFrame) -> DataFrame:
    monthly = (
        silver_accidents.withColumn("accident_year_month", _year_month("accident_date"))
        .groupBy("accident_year_month")
        .agg(
            F.count("*").alias("number_of_accidents"),
            F.round(F.avg("accident_hour")).alias("average_accident_hour"),
            F.max("borough").alias("most_common_borough"),
            F.max("zip_code").alias("most_common_zip_code"),
        )
    )

    w = Window.orderBy("accident_year_month")
    return monthly.withColumn(
        "pct_change_number_of_accidents", _pct_change("number_of_accidents", w)
    ).withColumn(
        "3m_rolling_avg_number_of_accidents",
        F.round(F.avg("number_of_accidents").over(w.rowsBetween(-3, 0))),
    )


# --- Policies ----------------------------------------------------------------


def build_monthly_policies(silver_policies: DataFrame) -> DataFrame:
    issued_policies = (
        silver_policies.withColumn("year_month", _year_month("issue_date"))
        .withColumn("issue_age_of_vehicle", F.year("issue_date") - F.col("model_year").cast("integer"))
        .groupBy("year_month")
        .agg(
            F.count("*").alias("policies_issued"),
            F.round(F.sum("sum_insured")).alias("exposure"),
            F.round(F.avg("issue_age_of_vehicle")).alias("avg_issue_age_of_vehicle"),
        )
    )

    expired_policies = (
        silver_policies.withColumn("year_month", _year_month("expiry_date"))
        .groupBy("year_month")
        .agg(F.count("*").alias("policies_expired"))
    )

    return (
        issued_policies.join(expired_policies, on="year_month", how="left")
        .fillna(0, ["policies_issued", "policies_expired"])
        .orderBy("year_month")
    )


def _write(df: DataFrame, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table_name)


def run_gold(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Read all Silver tables, aggregate, and write ``gold_*`` Delta tables."""

    silver_claims = spark.table("silver_claims")
    _write(build_daily_claims(silver_claims), "gold_claims_daily")
    _write(build_weekly_claims(silver_claims), "gold_claims_weekly")
    _write(build_monthly_claims(silver_claims), "gold_claims_monthly")

    silver_accidents = spark.table("silver_accidents")
    _write(build_daily_accidents(silver_accidents), "gold_accidents_daily")
    _write(build_weekly_accidents(silver_accidents), "gold_accidents_weekly")
    _write(build_monthly_accidents(silver_accidents), "gold_accidents_monthly")

    silver_policies = spark.table("silver_policies")
    _write(build_monthly_policies(silver_policies), "gold_policies_monthly")
