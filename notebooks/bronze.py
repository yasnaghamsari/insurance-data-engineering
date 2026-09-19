# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC The Bronze layer is the first layer of the Medallion Architecture.
# MAGIC
# MAGIC Its main responsibility is to ingest raw source data and store it as Delta tables with minimal processing.
# MAGIC
# MAGIC In this implementation:
# MAGIC - Data is loaded directly from raw source files.
# MAGIC - Delta Live Tables (DLT) is not used.
# MAGIC - Fivetran is not used.
# MAGIC - PySpark DataFrame API is used for all ingestion processes.
# MAGIC
# MAGIC The actual ingestion logic lives in `src/insurance_pipeline/bronze.py` so
# MAGIC that it's shared with the local/Airflow/CI runs instead of only existing
# MAGIC in this notebook. This notebook is a thin Databricks entrypoint.
# MAGIC
# MAGIC Bronze layer responsibilities:
# MAGIC - Read raw files
# MAGIC - Preserve source data structure
# MAGIC - Apply minimal ingestion-level processing
# MAGIC - Add ingestion metadata
# MAGIC - Store raw data as Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC
# MAGIC Install the `insurance_pipeline` package from this repo checkout (e.g. a
# MAGIC Databricks Repo) in editable mode, then restart Python so the import
# MAGIC below picks it up.

# COMMAND ----------

# MAGIC %pip install -e ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from insurance_pipeline import bronze
from insurance_pipeline.config import PipelineConfig

cfg = PipelineConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Policies, Claims, and Accidents
# MAGIC
# MAGIC `run_bronze` reads the MySQL/MongoDB/S3 sample sources, adds an
# MAGIC `ingestion_timestamp` column, and writes `bronze_policies`,
# MAGIC `bronze_claims`, and `bronze_accidents` as Delta tables.

# COMMAND ----------

bronze.run_bronze(spark, cfg)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer Validation
# MAGIC
# MAGIC Validate that all Bronze Delta tables have been successfully created.

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

spark.table("bronze_policies").printSchema()

# COMMAND ----------

spark.table("bronze_claims").printSchema()

# COMMAND ----------

spark.table("bronze_accidents").printSchema()

# COMMAND ----------

display(spark.table("bronze_policies").limit(5))

# COMMAND ----------

display(spark.table("bronze_claims").limit(5))

# COMMAND ----------

display(spark.table("bronze_accidents").limit(5))
