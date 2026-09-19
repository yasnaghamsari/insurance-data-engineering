"""Bronze layer: raw ingestion with minimal processing.

Ported from the original ``notebooks/bronze.py`` cells. Behavior is unchanged;
only the hardcoded workspace paths were replaced with :class:`PipelineConfig`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from insurance_pipeline.config import PipelineConfig

LAYER = "bronze"


def ingest_raw_data(spark: SparkSession, path: str, file_format: str = "csv") -> DataFrame:
    """Load raw source data into Bronze layer.

    Parameters
    ----------
    spark : SparkSession
    path : str
        Raw dataset path.
    file_format : str
        Source file format ("csv" or "json").

    Returns
    -------
    DataFrame
        Ingested raw dataframe with technical columns removed and an
        ``ingestion_timestamp`` column added.
    """
    if file_format == "csv":
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    elif file_format == "json":
        df = spark.read.json(path)
    else:
        raise ValueError("Unsupported file format")

    # Remove technical columns if present
    cols = [c for c in df.columns if not c.startswith("_")]
    df = df.select(*cols)

    # Add ingestion metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return df


def ingest_policies(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return ingest_raw_data(spark, cfg.policies_csv_path, file_format="csv")


def ingest_claims(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = spark.read.option("multiline", True).json(cfg.claims_json_path)

    cols = [c for c in df.columns if not c.startswith("_")]
    df = df.select(*cols)

    return df.withColumn("ingestion_timestamp", current_timestamp())


def ingest_accidents(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return ingest_raw_data(spark, cfg.accidents_csv_path, file_format="csv")


def run_bronze(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Ingest all three sources and write ``bronze_*`` Delta tables."""

    (
        ingest_policies(spark, cfg)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("bronze_policies")
    )

    (
        ingest_claims(spark, cfg)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("bronze_claims")
    )

    (
        ingest_accidents(spark, cfg)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("bronze_accidents")
    )
