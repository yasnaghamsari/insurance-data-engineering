"""Loads Gold Delta tables into ClickHouse for Metabase to query.

The README has always documented ClickHouse as the destination for the Gold
layer, but no code for that step existed anywhere in the repo. Table DDL
lives in ``docker/clickhouse/init.sql`` (applied when the ClickHouse
container starts); this module only truncates and re-loads rows.
"""

from __future__ import annotations

import clickhouse_connect
from pyspark.sql import SparkSession

from insurance_pipeline.config import PipelineConfig

GOLD_TABLES = [
    "gold_claims_daily",
    "gold_claims_weekly",
    "gold_claims_monthly",
    "gold_accidents_daily",
    "gold_accidents_weekly",
    "gold_accidents_monthly",
    "gold_policies_monthly",
]


def get_client(cfg: PipelineConfig):
    return clickhouse_connect.get_client(
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
        database=cfg.clickhouse_database,
    )


def load_table(spark: SparkSession, client, table_name: str) -> int:
    pdf = spark.table(table_name).toPandas()
    client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")
    if not pdf.empty:
        client.insert_df(table_name, pdf)
    return len(pdf)


def run_clickhouse_load(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    client = get_client(cfg)
    try:
        return {table_name: load_table(spark, client, table_name) for table_name in GOLD_TABLES}
    finally:
        client.close()
