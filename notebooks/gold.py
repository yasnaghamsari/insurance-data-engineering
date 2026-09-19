# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC
# MAGIC Business-ready aggregations built from the Silver tables: daily/weekly/
# MAGIC monthly claims and accident metrics, and monthly policy metrics.
# MAGIC
# MAGIC The aggregation logic lives in `src/insurance_pipeline/gold.py`, shared
# MAGIC with the local/Airflow/CI runs. This notebook is a thin Databricks entrypoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from insurance_pipeline.config import PipelineConfig
from insurance_pipeline.gold import run_gold

cfg = PipelineConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Gold aggregations
# MAGIC
# MAGIC Reads `silver_claims`, `silver_accidents`, `silver_policies`; writes
# MAGIC `gold_claims_{daily,weekly,monthly}`, `gold_accidents_{daily,weekly,monthly}`,
# MAGIC and `gold_policies_monthly`.

# COMMAND ----------

run_gold(spark, cfg)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer Validation

# COMMAND ----------

for table_name in (
    "gold_claims_daily",
    "gold_claims_weekly",
    "gold_claims_monthly",
    "gold_accidents_daily",
    "gold_accidents_weekly",
    "gold_accidents_monthly",
    "gold_policies_monthly",
):
    print(table_name, spark.table(table_name).count())

# COMMAND ----------

spark.table("gold_claims_monthly").orderBy("claim_year_month").show(10)

# COMMAND ----------

spark.table("gold_policies_monthly").orderBy("year_month").show(10)
