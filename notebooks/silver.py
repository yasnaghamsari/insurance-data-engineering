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
# MAGIC
# MAGIC The transform logic lives in `src/insurance_pipeline/silver.py`, shared
# MAGIC with the local/Airflow/CI runs. This notebook is a thin Databricks entrypoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from insurance_pipeline.config import PipelineConfig
from insurance_pipeline.silver import run_silver

cfg = PipelineConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Silver transforms
# MAGIC
# MAGIC Reads `bronze_claims`, `bronze_policies`, `bronze_accidents`; writes
# MAGIC `silver_claims`, `silver_policies`, `silver_accidents`.

# COMMAND ----------

run_silver(spark, cfg)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer Validation

# COMMAND ----------

spark.table("silver_claims").printSchema()
spark.table("silver_claims").count()

# COMMAND ----------

spark.table("silver_policies").printSchema()
spark.table("silver_policies").count()

# COMMAND ----------

spark.table("silver_accidents").printSchema()
spark.table("silver_accidents").count()
