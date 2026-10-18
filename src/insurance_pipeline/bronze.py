"""Bronze layer: raw ingestion with minimal processing.

Reads each raw source as-is, drops internal ``_``-prefixed columns, stamps
``ingestion_timestamp``, and writes the ``bronze_*`` Delta tables.

Where policies and claims come from depends on ``PipelineConfig.source_mode``:
the sample files in ``files`` mode, or MySQL (policies, over JDBC) and MongoDB
(claims, via pymongo) in ``databases`` mode. Both modes produce the same
Bronze schema, since the databases are seeded from the same sample files.
Accidents are always read from a local file.
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from insurance_pipeline.config import PipelineConfig

LAYER = "bronze"


def _finalize_raw(df: DataFrame) -> DataFrame:
    """Drop technical ``_``-prefixed columns (e.g. Mongo's ``_id``) and stamp
    ``ingestion_timestamp``."""
    cols = [c for c in df.columns if not c.startswith("_")]
    return df.select(*cols).withColumn("ingestion_timestamp", current_timestamp())


def ingest_raw_data(
    spark: SparkSession, path: str, file_format: str = "csv", multiline: bool = False
) -> DataFrame:
    """Load raw source data into Bronze layer.

    Parameters
    ----------
    spark : SparkSession
    path : str
        Raw dataset path.
    file_format : str
        Source file format ("csv" or "json").
    multiline : bool
        Allow quoted CSV fields to span lines. Off by default, since it stops
        Spark from splitting the file across tasks.

    Returns
    -------
    DataFrame
        Ingested raw dataframe with technical columns removed and an
        ``ingestion_timestamp`` column added.
    """
    if file_format == "csv":
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("multiLine", multiline)
            .csv(path)
        )
    elif file_format == "json":
        df = spark.read.json(path)
    else:
        raise ValueError("Unsupported file format")

    return _finalize_raw(df)


def mysql_jdbc_options(cfg: PipelineConfig) -> dict[str, str]:
    return {
        "url": cfg.mysql_jdbc_url,
        "dbtable": cfg.mysql_policies_table,
        "user": cfg.mysql_user,
        "password": cfg.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def ingest_policies(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    if cfg.source_mode == "databases":
        return _finalize_raw(spark.read.format("jdbc").options(**mysql_jdbc_options(cfg)).load())
    # 22 records in the policies CSV have a quoted CHASSIS_NO ending in a
    # newline. Without multiline, Spark splits each into two broken rows that
    # Silver then drops, while MySQL's LOAD DATA (databases mode) reads them
    # correctly.
    return ingest_raw_data(spark, cfg.policies_csv_path, file_format="csv", multiline=True)


def fetch_mongo_claims(cfg: PipelineConfig) -> list[str]:
    """Every document in the claims collection, as one JSON string each.

    Imported lazily so file-mode runs don't need a Mongo client at all.
    """
    from pymongo import MongoClient

    client = MongoClient(cfg.mongo_uri)
    try:
        collection = client[cfg.mongo_database][cfg.mongo_claims_collection]
        return [json.dumps(doc, default=str) for doc in collection.find({}, {"_id": 0})]
    finally:
        client.close()


def claims_from_json_documents(spark: SparkSession, documents: list[str]) -> DataFrame:
    """Build the raw claims DataFrame from JSON documents.

    Going through ``spark.read.json`` (rather than ``createDataFrame`` on the
    dicts) means Spark infers the schema exactly the way it does for the
    sample file in ``files`` mode, nested structs included.
    """
    return spark.read.json(spark.sparkContext.parallelize(documents))


def ingest_claims(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    if cfg.source_mode == "databases":
        df = claims_from_json_documents(spark, fetch_mongo_claims(cfg))
    else:
        df = spark.read.option("multiline", True).json(cfg.claims_json_path)
    return _finalize_raw(df)


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
