# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Claims, Policies, and Accidents
# MAGIC
# MAGIC This notebook transforms Bronze insurance tables into cleaned Silver Delta tables.
# MAGIC
# MAGIC Each dataset follows the same pattern:
# MAGIC
# MAGIC 1. Read the Bronze table.
# MAGIC 2. Standardize names and data types.
# MAGIC 3. Add dataset-specific derived fields.
# MAGIC 4. Apply data-quality filters.
# MAGIC 5. Write the Silver Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Shared imports, schema loading, column-standardization helpers, validation helpers, and Delta write helpers.

# COMMAND ----------

import json
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

SCHEMA_PATH = "/Workspace/Users/wangwangming1975@gmail.com/insurance-claims/data/schemas"

# COMMAND ----------

def load_schema(file_name):
    with open(f"{SCHEMA_PATH}/{file_name}", "r", encoding="utf-8") as schema_file:
        return json.load(schema_file)


def get_column_case_insensitive(df, expected_name):
    matches = [column for column in df.columns if column.lower() == expected_name.lower()]
    if not matches:
        raise ValueError(f"Column '{expected_name}' was not found. Available columns: {df.columns}")
    return matches[0]


def rename_from_schema(df, schema, use_alias=True):
    for spec in schema:
        source_col = spec["name"]
        target_col = spec.get("alias", source_col) if use_alias else source_col
        actual_col = get_column_case_insensitive(df, source_col)

        if actual_col != target_col:
            df = df.withColumnRenamed(actual_col, target_col)

    return df


def require_non_null(df, columns):
    required_condition = reduce(
        lambda condition, column: condition & F.col(column).isNotNull(),
        columns,
        F.lit(True),
    )
    return df.filter(required_condition)


def write_silver_table(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Claims
# MAGIC
# MAGIC Clean claim records by flattening nested Bronze fields, standardizing dates, creating analysis flags, and writing `silver_claims`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Claims
# MAGIC
# MAGIC Read the raw claims source table.

# COMMAND ----------

claims_df = spark.table("bronze_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform Claims
# MAGIC
# MAGIC Flatten nested claim, collision, driver, and incident structures. Standardize identifiers during selection so a separate rename step is not needed.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize Claim Types
# MAGIC
# MAGIC Convert date and timestamp columns to Spark types. Malformed license issue dates are returned as null.

# COMMAND ----------

silver_claims_df = (
    silver_claims_df
    .withColumn("claim_datetime", F.to_timestamp("claim_datetime", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("incident_date", F.to_date("incident_date", "dd-MM-yyyy"))
    .withColumn(
        "license_issue_date",
        F.when(
            F.col("license_issue_date").rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}$"),
            F.to_date("license_issue_date", "dd-MM-yyyy"),
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Claim Features
# MAGIC
# MAGIC Add derived columns used by downstream claim analysis.

# COMMAND ----------

silver_claims_df = silver_claims_df.withColumn(
    "injury_to_person",
    F.when(F.col("injury_claim_amount") > 0, F.lit(1)).otherwise(F.lit(0)),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Claims
# MAGIC
# MAGIC Keep records with required identifiers, a valid incident date, and an incident hour between 0 and 24.

# COMMAND ----------

silver_claims_df = require_non_null(
    silver_claims_df,
    ["claim_number", "policy_number", "incident_date"],
).filter(F.col("incident_hour").between(0, 24))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver Claims
# MAGIC
# MAGIC Overwrite `silver_claims` with the cleaned claims dataset.

# COMMAND ----------

write_silver_table(silver_claims_df, "silver_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Policies
# MAGIC
# MAGIC Clean policy records by applying schema aliases, parsing dates, validating required business fields, and writing `silver_policies`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Policies
# MAGIC
# MAGIC Load the policy schema and the raw policies source table.

# COMMAND ----------

policies_schema = load_schema("sql/policies.json")
policies_df = spark.table("bronze_policies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize Policy Fields
# MAGIC
# MAGIC Apply schema aliases and convert `driver_dob` to a Spark date, returning null for malformed values.

# COMMAND ----------

policies_df = rename_from_schema(policies_df, policies_schema)
policies_df = policies_df.withColumn(
    "driver_dob",
    F.expr("try_to_date(driver_dob, 'dd-MM-yyyy')"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Policies
# MAGIC
# MAGIC Keep policies with non-null customer, policy, issue-date, premium, and insured-sum fields.

# COMMAND ----------

policies_df = require_non_null(
    policies_df,
    ["customer_id", "policy_number", "issue_date", "premium", "sum_insured"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver Policies
# MAGIC
# MAGIC Overwrite `silver_policies` with the validated policy dataset.

# COMMAND ----------

write_silver_table(policies_df, "silver_policies")

# COMMAND ----------

spark.table("silver_policies").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Accidents
# MAGIC
# MAGIC Clean accident records by standardizing schema column names, casting numeric fields, validating required location fields, and writing `silver_accidents`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Accidents
# MAGIC
# MAGIC Load the accidents schema and the raw accidents source table.

# COMMAND ----------

accidents_schema = load_schema("s3/accidents.json")
accidents_df = spark.table("bronze_accidents")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize Accident Fields
# MAGIC
# MAGIC Apply schema column names and cast date, ZIP code, injury, fatality, and collision identifier fields.

# COMMAND ----------

accidents_df = rename_from_schema(accidents_df, accidents_schema, use_alias=False)

accidents_df = (
    accidents_df
    .withColumn("accident_date", F.to_timestamp("accident_date"))
    .withColumn("zip_code", F.expr("try_cast(zip_code as double)").cast("int"))
    .withColumn("number_of_persons_injured", F.expr("try_cast(number_of_persons_injured as int)"))
    .withColumn("number_of_persons_killed", F.expr("try_cast(number_of_persons_killed as int)"))
    .withColumn("number_of_motorist_injured", F.expr("try_cast(number_of_motorist_injured as int)"))
    .withColumn("number_of_motorist_killed", F.expr("try_cast(number_of_motorist_killed as int)"))
    .withColumn("collision_id", F.col("collision_id").cast("string"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Accidents
# MAGIC
# MAGIC Keep records with a valid accident timestamp, collision identifier, latitude, and longitude.

# COMMAND ----------

accidents_df = require_non_null(
    accidents_df,
    ["accident_date", "collision_id", "latitude", "longitude"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver Accidents
# MAGIC
# MAGIC Overwrite `silver_accidents` with the cleaned accidents dataset.

# COMMAND ----------

write_silver_table(accidents_df, "silver_accidents")

# COMMAND ----------

spark.table("silver_accidents").count()