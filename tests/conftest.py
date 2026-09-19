import shutil
import tempfile
from pathlib import Path

import pytest

from insurance_pipeline import bronze, gold, silver
from insurance_pipeline.config import PipelineConfig
from insurance_pipeline.spark_utils import get_local_spark_session


@pytest.fixture(scope="session")
def warehouse_dir():
    path = Path(tempfile.mkdtemp(prefix="insurance_pipeline_test_warehouse_"))
    yield str(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def cfg(warehouse_dir):
    return PipelineConfig(warehouse_dir=warehouse_dir)


@pytest.fixture(scope="session")
def spark(cfg):
    session = get_local_spark_session(cfg, app_name="insurance-pipeline-tests")
    yield session
    session.stop()


# Session-scoped, dependency-chained fixtures so each layer only runs once
# regardless of which test file exercises it first.


@pytest.fixture(scope="session")
def bronze_tables(spark, cfg):
    bronze.run_bronze(spark, cfg)
    return True


@pytest.fixture(scope="session")
def silver_tables(spark, cfg, bronze_tables):
    silver.run_silver(spark, cfg)
    return True


@pytest.fixture(scope="session")
def gold_tables(spark, cfg, silver_tables):
    gold.run_gold(spark, cfg)
    return True
