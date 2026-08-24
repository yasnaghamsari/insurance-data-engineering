# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC
# MAGIC ## Claims Aggregations
# MAGIC
# MAGIC ### Daily Claims Aggregation
# MAGIC
# MAGIC Creates daily business-level metrics from silver claims data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

silver_claims = spark.table("silver_claims")

# COMMAND ----------

silver_claims.printSchema()

# COMMAND ----------

daily_claims = (
    silver_claims
    .withColumn(
        "claim_date",
        F.to_date("claim_datetime")
    )
    .groupBy("claim_date")
    .agg(
        F.count("*").alias("number_of_claims"),
        F.sum("total_claim_amount").alias("total_claim_amount"),
        F.sum("injury_claim_amount").alias("injury_claim_amount"),
        F.sum("property_claim_amount").alias("property_claim_amount"),
        F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
        F.round(
            F.avg("incident_hour")
        ).alias("average_incident_hour"),
        F.round(
            F.avg("driver_age")
        ).alias("average_driver_age")
    )
)

# COMMAND ----------



w = Window.orderBy("claim_date")

# COMMAND ----------

daily_claims = (
    daily_claims
    .withColumn(
        "pct_change_number_of_claims",
        F.round(
            (
                (F.col("number_of_claims") -
                 F.lag("number_of_claims").over(w))
                /
                F.lag("number_of_claims").over(w)
            ) * 100,
            2
        )
    )
    .withColumn(
        "30d_rolling_avg_total_claim_amount",
        F.round(
            F.avg("total_claim_amount")
            .over(w.rowsBetween(-30,0))
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekly Claims Aggregation
# MAGIC
# MAGIC Creates weekly-level business metrics from silver claims data.

# COMMAND ----------

weekly_claims = (
    silver_claims
    .withColumn(
        "claim_year_week",
        F.concat_ws(
            "-",
            F.year("claim_datetime"),
            F.lpad(
                F.weekofyear("claim_datetime"),
                2,
                "0"
            )
        )
    )
)

# COMMAND ----------

weekly_claims = (
    weekly_claims
    .groupBy("claim_year_week")
    .agg(
        F.count("*").alias("number_of_claims"),

        F.sum("total_claim_amount")
        .alias("total_claim_amount"),

        F.sum("injury_claim_amount")
        .alias("injury_claim_amount"),

        F.sum("property_claim_amount")
        .alias("property_claim_amount"),

        F.sum("vehicle_claim_amount")
        .alias("vehicle_claim_amount"),

        F.round(
            F.avg("incident_hour")
        ).alias("average_incident_hour"),

        F.round(
            F.avg("driver_age")
        ).alias("average_driver_age")
    )
)

# COMMAND ----------

w_week = Window.orderBy("claim_year_week")

# COMMAND ----------

weekly_claims = (
    weekly_claims
    .withColumn(
        "pct_change_number_of_claims",
        F.round(
            (
                (
                    F.col("number_of_claims")
                    -
                    F.lag("number_of_claims").over(w_week)
                )
                /
                F.lag("number_of_claims").over(w_week)
            ) * 100,
            2
        )
    )
    .withColumn(
        "3m_rolling_avg_total_claim_amount",
        F.round(
            F.avg("total_claim_amount")
            .over(
                w_week.rowsBetween(-3,0)
            )
        )
    )
)

# COMMAND ----------

weekly_claims.show(10)

# COMMAND ----------

weekly_claims.printSchema()

# COMMAND ----------

(
    weekly_claims.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_claims_weekly")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Claims Aggregation
# MAGIC
# MAGIC Creates monthly-level business metrics from silver claims data.

# COMMAND ----------

monthly_claims = (
    silver_claims
    .withColumn(
        "claim_year_month",
        F.concat_ws(
            "-",
            F.year("claim_datetime"),
            F.lpad(
                F.month("claim_datetime"),
                2,
                "0"
            )
        )
    )
)

# COMMAND ----------

monthly_claims = (
    monthly_claims
    .groupBy("claim_year_month")
    .agg(
        F.count("*")
        .alias("number_of_claims"),

        F.sum("total_claim_amount")
        .alias("total_claim_amount"),

        F.sum("injury_claim_amount")
        .alias("injury_claim_amount"),

        F.sum("property_claim_amount")
        .alias("property_claim_amount"),

        F.sum("vehicle_claim_amount")
        .alias("vehicle_claim_amount"),

        F.round(
            F.avg("incident_hour")
        ).alias("average_incident_hour"),

        F.round(
            F.avg("driver_age")
        ).alias("average_driver_age")
    )
)

# COMMAND ----------

w_month = Window.orderBy("claim_year_month")

# COMMAND ----------

monthly_claims = (
    monthly_claims
    .withColumn(
        "pct_change_number_of_claims",
        F.round(
            (
                (
                    F.col("number_of_claims")
                    -
                    F.lag("number_of_claims").over(w_month)
                )
                /
                F.lag("number_of_claims").over(w_month)
            ) * 100,
            2
        )
    )
    .withColumn(
        "3m_rolling_avg_total_claim_amount",
        F.round(
            F.avg("total_claim_amount")
            .over(
                w_month.rowsBetween(-3,0)
            )
        )
    )
)

# COMMAND ----------

monthly_claims.show(10)

# COMMAND ----------

monthly_claims.printSchema()

# COMMAND ----------

(
    monthly_claims.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_claims_monthly")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Accident Aggregations
# MAGIC
# MAGIC Business-level accident metrics generated from silver accident records.

# COMMAND ----------

silver_accidents = spark.table("silver_accidents")

# COMMAND ----------

silver_accidents.printSchema()

# COMMAND ----------

daily_accidents = (
    silver_accidents
    .groupBy("accident_date")
    .agg(
        F.count("*")
        .alias("number_of_accidents"),

        F.round(
            F.avg("accident_hour")
        )
        .alias("average_accident_hour"),

        F.expr(
            "percentile(number_of_vehicles_involved, array(0.75))[0]"
        )
        .alias("75_pctl_number_of_vehicles_involved"),

        F.max("borough")
        .alias("most_common_borough"),

        F.max("zip_code")
        .alias("most_common_zip_code")
    )
)

# COMMAND ----------



# COMMAND ----------

w_accident = Window.orderBy("accident_date")

# COMMAND ----------



# COMMAND ----------

silver_accidents = (
    silver_accidents
    .withColumn(
        "accident_hour",
        F.hour(
            F.try_to_timestamp(
                F.col("accident_time"),
                F.lit("H:mm")
            )
        )
    )
)

# COMMAND ----------

daily_accidents = (
    daily_accidents

    .withColumn(
        "pct_change_number_of_accidents",
        F.round(
            (
                (
                    F.col("number_of_accidents")
                    -
                    F.lag("number_of_accidents")
                    .over(w_accident)
                )
                /
                F.lag("number_of_accidents")
                .over(w_accident)
            ) * 100,
            2
        )
    )

    .withColumn(
        "30d_rolling_avg_number_of_accidents",
        F.round(
            F.avg("number_of_accidents")
            .over(
                w_accident.rowsBetween(-30,0)
            )
        )
    )
)

# COMMAND ----------

silver_accidents.select(
    "accident_time",
    "accident_hour"
).show(10)

# COMMAND ----------

silver_accidents.select(
    "accident_time",
    "accident_hour"
).show(10)

# COMMAND ----------

daily_accidents = (
    silver_accidents
    .groupBy("accident_date")
    .agg(
        F.count("*").alias("number_of_accidents"),

        F.round(
            F.avg("accident_hour")
        ).alias("average_accident_hour"),

        F.max("borough")
        .alias("most_common_borough"),

        F.max("zip_code")
        .alias("most_common_zip_code")
    )
)

# COMMAND ----------

w_accident = Window.orderBy("accident_date")

# COMMAND ----------

(
    daily_accidents.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_accidents_daily")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekly Accident Aggregation
# MAGIC
# MAGIC Creates weekly-level accident metrics from silver accident records.

# COMMAND ----------

weekly_accidents = (
    silver_accidents
    .withColumn(
        "accident_year_week",
        F.concat_ws(
            "-",
            F.year("accident_date"),
            F.lpad(
                F.weekofyear("accident_date"),
                2,
                "0"
            )
        )
    )
)

# COMMAND ----------

weekly_accidents = (
    weekly_accidents
    .groupBy("accident_year_week")
    .agg(
        F.count("*")
        .alias("number_of_accidents"),

        F.round(
            F.avg("accident_hour")
        )
        .alias("average_accident_hour"),

        F.max("borough")
        .alias("most_common_borough"),

        F.max("zip_code")
        .alias("most_common_zip_code")
    )
)

# COMMAND ----------

w_accident_week = Window.orderBy("accident_year_week")

# COMMAND ----------

weekly_accidents = (
    weekly_accidents

    .withColumn(
        "pct_change_number_of_accidents",
        F.round(
            (
                (
                    F.col("number_of_accidents")
                    -
                    F.lag("number_of_accidents")
                    .over(w_accident_week)
                )
                /
                F.lag("number_of_accidents")
                .over(w_accident_week)
            ) * 100,
            2
        )
    )

    .withColumn(
        "3m_rolling_avg_number_of_accidents",
        F.round(
            F.avg("number_of_accidents")
            .over(
                w_accident_week.rowsBetween(-3,0)
            )
        )
    )
)

# COMMAND ----------

weekly_accidents.show(10)

# COMMAND ----------

weekly_accidents.printSchema()

# COMMAND ----------

(
    weekly_accidents.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_accidents_weekly")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Accident Aggregation
# MAGIC
# MAGIC Creates monthly-level accident metrics from silver accident records.

# COMMAND ----------

monthly_accidents = (
    silver_accidents
    .withColumn(
        "accident_year_month",
        F.concat_ws(
            "-",
            F.year("accident_date"),
            F.lpad(
                F.month("accident_date"),
                2,
                "0"
            )
        )
    )
)

# COMMAND ----------

monthly_accidents = (
    monthly_accidents
    .groupBy("accident_year_month")
    .agg(
        F.count("*")
        .alias("number_of_accidents"),

        F.round(
            F.avg("accident_hour")
        )
        .alias("average_accident_hour"),

        F.max("borough")
        .alias("most_common_borough"),

        F.max("zip_code")
        .alias("most_common_zip_code")
    )
)

# COMMAND ----------

w_accident_month = Window.orderBy("accident_year_month")

# COMMAND ----------

monthly_accidents = (
    monthly_accidents

    .withColumn(
        "pct_change_number_of_accidents",
        F.round(
            (
                (
                    F.col("number_of_accidents")
                    -
                    F.lag("number_of_accidents")
                    .over(w_accident_month)
                )
                /
                F.lag("number_of_accidents")
                .over(w_accident_month)
            ) * 100,
            2
        )
    )

    .withColumn(
        "3m_rolling_avg_number_of_accidents",
        F.round(
            F.avg("number_of_accidents")
            .over(
                w_accident_month.rowsBetween(-3,0)
            )
        )
    )
)

# COMMAND ----------

monthly_accidents.show(10)

# COMMAND ----------

monthly_accidents.printSchema()

# COMMAND ----------

(
    monthly_accidents.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_accidents_monthly")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Policy Aggregation
# MAGIC
# MAGIC Creates monthly-level policy business metrics from silver policy records.

# COMMAND ----------

silver_policies = spark.table("silver_policies")

# COMMAND ----------

silver_policies.printSchema()

# COMMAND ----------

monthly_policies = (
    silver_policies
    .withColumn(
        "year_month",
        F.concat_ws(
            "-",
            F.year("issue_date"),
            F.lpad(
                F.month("issue_date"),
                2,
                "0"
            )
        )
    )
)

# COMMAND ----------

issued_policies = (
    monthly_policies
    .withColumn(
        "issue_age_of_vehicle",
        F.year("issue_date") - F.col("model_year").cast("integer")
    )
    .groupBy("year_month")
    .agg(
        F.count("*")
        .alias("policies_issued"),

        F.round(
            F.sum("sum_insured")
        )
        .alias("exposure"),

        F.round(
            F.avg("issue_age_of_vehicle")
        )
        .alias("avg_issue_age_of_vehicle")
    )
)

# COMMAND ----------

expired_policies = (
    silver_policies
    .withColumn(
        "year_month",
        F.concat_ws(
            "-",
            F.year("expiry_date"),
            F.lpad(
                F.month("expiry_date"),
                2,
                "0"
            )
        )
    )
    .groupBy("year_month")
    .agg(
        F.count("*")
        .alias("policies_expired")
    )
)

# COMMAND ----------

gold_policies_monthly = (
    issued_policies
    .join(
        expired_policies,
        on="year_month",
        how="left"
    )
    .fillna(
        0,
        [
            "policies_issued",
            "policies_expired"
        ]
    )
    .orderBy("year_month")
)

# COMMAND ----------

gold_policies_monthly.show(10)

# COMMAND ----------

gold_policies_monthly.printSchema()

# COMMAND ----------

(
    gold_policies_monthly.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_policies_monthly")
)