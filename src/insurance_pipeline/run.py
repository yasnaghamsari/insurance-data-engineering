"""CLI entrypoint used for local runs and by the Airflow DAG.

    python -m insurance_pipeline.run {bronze,silver,quality,gold,clickhouse,all}
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from insurance_pipeline import bronze, gold, quality
from insurance_pipeline.clickhouse_loader import run_clickhouse_load
from insurance_pipeline.config import PipelineConfig
from insurance_pipeline.silver import run_silver
from insurance_pipeline.spark_utils import get_local_spark_session

STEPS = ("bronze", "silver", "quality", "gold", "clickhouse")


def run_step(step: str, spark: SparkSession, cfg: PipelineConfig) -> None:
    if step == "bronze":
        bronze.run_bronze(spark, cfg)
    elif step == "silver":
        run_silver(spark, cfg)
    elif step == "quality":
        quality.run_quality_checks(spark, cfg)
    elif step == "gold":
        gold.run_gold(spark, cfg)
    elif step == "clickhouse":
        run_clickhouse_load(spark, cfg)
    else:
        raise ValueError(f"Unknown step: {step}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=(*STEPS, "all"))
    args = parser.parse_args(argv)

    cfg = PipelineConfig()
    spark = get_local_spark_session(cfg)

    steps = STEPS if args.step == "all" else (args.step,)
    for step in steps:
        run_step(step, spark, cfg)

    return 0


if __name__ == "__main__":
    sys.exit(main())
