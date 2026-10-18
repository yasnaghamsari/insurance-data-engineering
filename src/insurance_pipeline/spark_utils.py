"""SparkSession construction.

The local CLI, Airflow tasks, and pytest all build their SparkSession here,
with Delta Lake configured. The session runs in local mode unless
``SPARK_MASTER`` points at a standalone cluster (see ``cluster_spark_conf``).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from insurance_pipeline.config import PipelineConfig

MYSQL_JDBC_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"


def extra_spark_packages(cfg: PipelineConfig) -> list[str]:
    """Maven packages needed beyond Delta Lake. The MySQL JDBC driver is only
    pulled in when Bronze actually reads from MySQL, so file-mode runs (tests,
    CI) don't need to download it."""
    return [MYSQL_JDBC_PACKAGE] if cfg.source_mode == "databases" else []


def cluster_spark_conf(cfg: PipelineConfig) -> dict[str, str]:
    """Settings only needed when executors run outside the driver's process.

    On a standalone cluster, executors open connections *back* to the driver
    (task results, broadcasts, shuffle metadata), so the driver has to
    advertise a hostname they can resolve (``spark.driver.host``) while
    listening on every interface of its own container
    (``spark.driver.bindAddress``), on fixed ports rather than random ones.
    Empty in local mode, where everything stays inside one JVM.
    """
    if cfg.is_local_spark:
        return {}
    conf = {
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.executor.memory": cfg.spark_executor_memory,
    }
    if cfg.spark_driver_host:
        conf["spark.driver.host"] = cfg.spark_driver_host
    if cfg.spark_driver_port:
        conf["spark.driver.port"] = cfg.spark_driver_port
    if cfg.spark_block_manager_port:
        conf["spark.blockManager.port"] = cfg.spark_block_manager_port
    return conf


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
        .master(cfg.spark_master)
    )
    for key, value in cluster_spark_conf(cfg).items():
        builder = builder.config(key, value)

    packages = extra_spark_packages(cfg)
    try:
        from delta import configure_spark_with_delta_pip

        # Sets spark.jars.packages itself, so extra packages have to go
        # through it rather than a separate .config() call it would overwrite.
        builder = configure_spark_with_delta_pip(builder, extra_packages=packages)
    except ImportError:
        # delta-spark not installed (e.g. lightweight lint-only environments).
        if packages:
            builder = builder.config("spark.jars.packages", ",".join(packages))

    return builder.getOrCreate()
