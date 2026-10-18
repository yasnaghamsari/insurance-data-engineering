"""Silver layer: cleaning and standardization.

Each dataset follows the same pattern:
read the Bronze table, standardize names/types, add derived fields, apply
data-quality filters, write the Silver Delta table.
"""

from __future__ import annotations

import json
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from insurance_pipeline.config import PipelineConfig

# Minimum US driving age; anything below it in driver.age is a placeholder.
MIN_DRIVER_AGE = 16
# The source system's "unknown licence date" placeholder.
LICENSE_DATE_SENTINEL = "01-01-1900"


def load_schema(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as schema_file:
        return json.load(schema_file)


def get_column_case_insensitive(df: DataFrame, expected_name: str) -> str:
    matches = [column for column in df.columns if column.lower() == expected_name.lower()]
    if not matches:
        raise ValueError(f"Column '{expected_name}' was not found. Available columns: {df.columns}")
    return matches[0]


def rename_from_schema(df: DataFrame, schema: list[dict], use_alias: bool = True) -> DataFrame:
    for spec in schema:
        source_col = spec["name"]
        target_col = spec.get("alias", source_col) if use_alias else source_col
        actual_col = get_column_case_insensitive(df, source_col)

        if actual_col != target_col:
            df = df.withColumnRenamed(actual_col, target_col)

    return df


def require_non_null(df: DataFrame, columns: list[str]) -> DataFrame:
    required_condition = reduce(
        lambda condition, column: condition & F.col(column).isNotNull(),
        columns,
        F.lit(True),
    )
    return df.filter(required_condition)


def write_silver_table(df: DataFrame, table_name: str) -> None:
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table_name))


def transform_claims(claims_df: DataFrame) -> DataFrame:
    silver_claims_df = claims_df.select(
        F.col("claim_no").alias("claim_number"),
        F.col("policy_no").alias("policy_number"),
        F.col("claim_datetime"),
        F.col("claim_amount.injury").alias("injury_claim_amount"),
        F.col("claim_amount.property").alias("property_claim_amount"),
        F.col("claim_amount.vehicle").alias("vehicle_claim_amount"),
        F.col("claim_amount.total").alias("total_claim_amount"),
        F.col("collision.number_of_vehicles_involved"),
        F.col("collision.type").alias("collision_type"),
        F.col("driver.age").alias("driver_age"),
        F.col("driver.insured_relationship"),
        F.col("driver.license_issue_date"),
        F.col("incident.date").alias("incident_date"),
        F.col("incident.hour").alias("incident_hour"),
        F.col("incident.severity"),
        F.col("incident.type").alias("incident_type"),
        F.col("months_as_customer"),
        F.col("number_of_witnesses"),
        F.col("suspicious_activity"),
        F.col("ingestion_timestamp"),
    )

    # The source system fills unknown values with sentinels rather than
    # leaving them empty: driver.age == 0 (888 of the 5,627 sample claims)
    # and driver.license_issue_date == "01-01-1900" (2,736 claims). Left
    # in, they'd drag average_driver_age down and read as 116-year-old
    # licences, so both become null. Any age under 16 (the minimum US
    # driving age) is treated the same way — on the sample data that only
    # adds 2 claims beyond the zeros.
    silver_claims_df = (
        silver_claims_df.withColumn("claim_datetime", F.to_timestamp("claim_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("incident_date", F.to_date("incident_date", "dd-MM-yyyy"))
        .withColumn(
            "license_issue_date",
            F.when(
                F.col("license_issue_date").rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}$")
                & (F.col("license_issue_date") != LICENSE_DATE_SENTINEL),
                F.to_date("license_issue_date", "dd-MM-yyyy"),
            ),
        )
        .withColumn("driver_age", F.when(F.col("driver_age") >= MIN_DRIVER_AGE, F.col("driver_age")))
    )

    silver_claims_df = silver_claims_df.withColumn(
        "injury_to_person",
        F.when(F.col("injury_claim_amount") > 0, F.lit(1)).otherwise(F.lit(0)),
    )

    silver_claims_df = require_non_null(
        silver_claims_df,
        ["claim_number", "policy_number", "incident_date"],
    ).filter(F.col("incident_hour").between(0, 23))

    return silver_claims_df


def deduplicate_policies(policies_df: DataFrame) -> DataFrame:
    """Keep exactly one row per ``policy_number``.

    The raw feed repeats 7 policy numbers (14 rows). Most are exact
    copies; the rest disagree on expiry date, premium or body type. Without
    this, every claim on one of those policies would fan out into one row
    per copy in the claims-to-policies join and be counted twice.

    Rule: keep the row with the latest ``issue_date`` (the most recent
    version of the policy), then the latest ``expiry_date``, then the
    highest ``premium`` and ``sum_insured``. Any remaining tie is broken by
    sorting on every other column, so the row kept never depends on how
    Spark happened to partition the input.
    """
    priority = [
        F.col("issue_date").desc_nulls_last(),
        F.col("expiry_date").desc_nulls_last(),
        F.col("premium").desc_nulls_last(),
        F.col("sum_insured").desc_nulls_last(),
    ]
    prioritized = {"policy_number", "issue_date", "expiry_date", "premium", "sum_insured"}
    tie_breakers = [
        F.col(c).asc_nulls_last()
        for c in sorted(policies_df.columns)
        if c not in prioritized and c != "ingestion_timestamp"
    ]
    window = Window.partitionBy("policy_number").orderBy(*priority, *tie_breakers)
    return (
        policies_df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def transform_policies(policies_df: DataFrame, policies_schema: list[dict]) -> DataFrame:
    policies_df = rename_from_schema(policies_df, policies_schema)
    # rename_from_schema only renames columns; it doesn't apply the "type"/
    # "format" the schema JSON declares for issue_date/effective_date/
    # expiry_date (dd-MM-yyyy strings), so without this they're left as
    # plain strings and every date-based Gold aggregation
    # (F.year(issue_date), etc.) would get null for every row.
    #
    # to_date() already returns null for a malformed string rather than
    # raising (Spark only throws on this under ANSI mode, which isn't
    # enabled here) so no try_to_date() is needed (that function isn't in
    # open-source Spark's function registry anyway).
    for date_col in ("issue_date", "effective_date", "expiry_date", "driver_dob"):
        policies_df = policies_df.withColumn(date_col, F.to_date(F.col(date_col), "dd-MM-yyyy"))

    # sum_insured is deliberately not required: it's null on 112,696 of the
    # 227,484 raw rows (49.5%), so requiring it would silently drop half
    # the book, and every policy's premium with it. Gold's exposure sums
    # simply skip the nulls.
    policies_df = require_non_null(
        policies_df,
        ["customer_id", "policy_number", "issue_date", "premium"],
    )

    # 10,199 raw rows have a negative premium, which isn't a valid written
    # premium for a policy (none have a negative sum_insured on the sample
    # data, but the same rule applies). These are dropped rather than let
    # into Gold aggregates. A null sum_insured is allowed; a negative one
    # is not.
    policies_df = policies_df.filter(
        (F.col("premium") >= 0) & (F.col("sum_insured").isNull() | (F.col("sum_insured") >= 0))
    )

    # Deduplicate after the validity filters, so an invalid copy of a
    # policy can never be the one kept (and then dropped, losing the key).
    return deduplicate_policies(policies_df)


def transform_accidents(accidents_df: DataFrame, accidents_schema: list[dict]) -> DataFrame:
    accidents_df = rename_from_schema(accidents_df, accidents_schema, use_alias=False)

    accidents_df = (
        accidents_df.withColumn("accident_date", F.to_timestamp("accident_date"))
        .withColumn("zip_code", F.expr("try_cast(zip_code as double)").cast("int"))
        .withColumn("number_of_persons_injured", F.expr("try_cast(number_of_persons_injured as int)"))
        .withColumn("number_of_persons_killed", F.expr("try_cast(number_of_persons_killed as int)"))
        .withColumn("number_of_motorist_injured", F.expr("try_cast(number_of_motorist_injured as int)"))
        .withColumn("number_of_motorist_killed", F.expr("try_cast(number_of_motorist_killed as int)"))
        .withColumn("collision_id", F.col("collision_id").cast("string"))
        .withColumn(
            "accident_hour",
            F.hour(F.try_to_timestamp(F.col("accident_time"), F.lit("H:mm"))),
        )
    )

    accidents_df = require_non_null(
        accidents_df,
        ["accident_date", "collision_id", "latitude", "longitude"],
    )

    # 104 rows of the 1.6M-row raw feed have a latitude/longitude outside
    # valid Earth coordinates (e.g. longitude beyond +/-180), which is
    # corrupted data rather than a real location. Silver's job is to clean
    # these out rather than let them reach Gold aggregates.
    accidents_df = accidents_df.filter(
        F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
    )

    return accidents_df


def run_silver(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Read all Bronze tables, transform, and write Silver Delta tables."""

    claims_df = spark.table("bronze_claims")
    write_silver_table(transform_claims(claims_df), "silver_claims")

    policies_schema = load_schema(cfg.policies_schema_path)
    policies_df = spark.table("bronze_policies")
    write_silver_table(transform_policies(policies_df, policies_schema), "silver_policies")

    accidents_schema = load_schema(cfg.accidents_schema_path)
    accidents_df = spark.table("bronze_accidents")
    write_silver_table(transform_accidents(accidents_df, accidents_schema), "silver_accidents")
