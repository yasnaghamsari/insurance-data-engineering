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
# MAGIC Import required PySpark libraries and configure the Bronze layer.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


LAYER = "bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Methods
# MAGIC
# MAGIC A reusable ingestion function is created to standardize loading raw datasets.
# MAGIC
# MAGIC This function:
# MAGIC - Reads CSV and JSON files
# MAGIC - Removes unnecessary technical columns
# MAGIC - Adds ingestion timestamp
# MAGIC - Returns a DataFrame ready for Bronze storage

# COMMAND ----------

def ingest_raw_data(path: str, file_format: str = "csv") -> DataFrame:
    """
    Load raw source data into Bronze layer.

    Parameters
    ----------
    path : str
        Raw dataset path.

    file_format : str
        Source file format.

    Returns
    -------
    DataFrame
        Ingested raw dataframe.
    """

    if file_format == "csv":

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )

    elif file_format == "json":

        df = spark.read.json(path)

    else:
        raise ValueError(
            "Unsupported file format"
        )


    # Remove technical columns if present
    cols = [
        c for c in df.columns
        if not c.startswith("_")
    ]

    df = df.select(*cols)


    # Add ingestion metadata
    df = df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )


    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policies - MySQL Source
# MAGIC
# MAGIC Policy records are loaded from the raw MySQL sample dataset.
# MAGIC
# MAGIC Source:
# MAGIC data/samples/mysql/policies.csv
# MAGIC
# MAGIC Bronze processing:
# MAGIC - Read CSV file
# MAGIC - Preserve original attributes
# MAGIC - Add ingestion timestamp
# MAGIC - Store as Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policies - MySQL Source
# MAGIC
# MAGIC Policy records are loaded from the raw MySQL sample dataset.
# MAGIC
# MAGIC Source:
# MAGIC data/samples/mysql/policies.csv
# MAGIC
# MAGIC Bronze processing:
# MAGIC - Read CSV file
# MAGIC - Preserve original attributes
# MAGIC - Add ingestion timestamp
# MAGIC - Store as Delta table

# COMMAND ----------

policies_df = ingest_raw_data(
    path="/Workspace/Users/wangwangming1975@gmail.com/insurance-claims/data/samples/mysql/policies.csv",
    file_format="csv"
)


(
    policies_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("bronze_policies")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims - MongoDB Source
# MAGIC
# MAGIC Claim records are loaded from the raw MongoDB sample dataset.
# MAGIC
# MAGIC Source:
# MAGIC data/samples/mongodb/claims.json
# MAGIC
# MAGIC Bronze processing:
# MAGIC - Read JSON file
# MAGIC - Preserve source structure
# MAGIC - Add ingestion timestamp
# MAGIC - Store as Delta table

# COMMAND ----------

claims_df = (
    spark.read
    .option("multiline", True)
    .json(
        "file:/Workspace/Users/wangwangming1975@gmail.com/insurance-claims/data/samples/s3/tmp/claims.json"
    )
)


# Remove technical columns if present
cols = [
    c for c in claims_df.columns
    if not c.startswith("_")
]

claims_df = claims_df.select(*cols)


# Add ingestion metadata
from pyspark.sql.functions import current_timestamp

claims_df = claims_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)


(
    claims_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("bronze_claims")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traffic Accidents - S3 Source
# MAGIC
# MAGIC Accident records are loaded from the raw S3 sample dataset.
# MAGIC
# MAGIC Source:
# MAGIC data/samples/s3/accidents.csv
# MAGIC
# MAGIC Bronze processing:
# MAGIC - Read CSV file
# MAGIC - Preserve raw attributes
# MAGIC - Add ingestion timestamp
# MAGIC - Store as Delta table

# COMMAND ----------

accidents_df = ingest_raw_data(
    path="file:/Workspace/Users/wangwangming1975@gmail.com/insurance-claims/data/samples/s3/external/accidents.csv.gz",
    file_format="csv"
)


(
    accidents_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("bronze_accidents")
)

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

# MAGIC %sh
# MAGIC head -5 /Workspace/Users/wangwangming1975@gmail.com/insurance-claims/data/samples/s3/tmp/claims.json

# COMMAND ----------

display(
    spark.table("bronze_policies").limit(5)
)

# COMMAND ----------

display(
    spark.table("bronze_claims").limit(5)
)

# COMMAND ----------

display(
    spark.table("bronze_accidents").limit(5)
)