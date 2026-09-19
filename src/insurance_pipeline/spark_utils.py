"""Local (non-Databricks) SparkSession construction.

On Databricks, notebooks use the runtime-injected ``spark`` object directly and
never call this module. Everywhere else (local CLI, Airflow tasks, pytest) we
need to build one ourselves, with Delta Lake configured.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from insurance_pipeline.config import PipelineConfig


def get_local_spark_session(cfg: PipelineConfig, app_name: str = "insurance-pipeline") -> SparkSession:
    # Without this, Spark spawns "python"/"python3" off PATH to launch worker
    # processes. On Windows that can resolve to the Microsoft Store's
    # python.exe app-execution-alias stub instead of a real interpreter,
    # which silently hangs every job until the JVM's accept() call times out.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Each CLI/Airflow invocation (python -m insurance_pipeline.run <step>) is
    # its own process with its own JVM. Spark's default in-memory catalog
    # only lives for that one process, so saveAsTable in one step and
    # spark.table(...) in the next (a *different* process) would otherwise
    # fail with TABLE_OR_VIEW_NOT_FOUND even though the Delta files are
    # sitting right there on disk. Hive support backed by a local Derby DB
    # gives the catalog somewhere durable to live, shared by every process
    # that points at the same warehouse_dir. (Not needed for pytest, which
    # keeps one SparkSession alive for the whole test session — but harmless
    # there too.)
    metastore_dir = Path(cfg.warehouse_dir).parent / "metastore_db"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", cfg.warehouse_dir)
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        )
        .config("spark.driver.memory", "2g")
        # Silver's date parsing (to_date/to_timestamp on messy source strings)
        # relies on malformed input becoming null rather than raising. Spark's
        # default policy raises SparkUpgradeException instead for the subset
        # of inputs where the legacy and new date parsers would disagree,
        # which is exactly the "not sure how to parse this" case we want
        # null for, not a crash.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .enableHiveSupport()
        .master("local[*]")
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        # delta-spark not installed (e.g. lightweight lint-only environments).
        pass

    return builder.getOrCreate()
