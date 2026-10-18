"""Environment-driven configuration.

Every path/connection detail is read from environment variables with sensible local defaults so
the same code runs on a laptop, in Docker Compose/Airflow, and in CI.

``SOURCE_MODE`` picks where Bronze reads policies and claims from:

- ``files`` (default, used by tests and CI): the sample files under ``DATA_ROOT``.
- ``databases`` (used by the Airflow DAG): policies from MySQL over JDBC, claims from
  MongoDB. Docker Compose seeds both from the same sample files, so the two modes see the
  same data.

Accidents are always read from a local file (see ``accidents_csv_path``).

``SPARK_MASTER`` picks where Spark runs: ``local[*]`` (default; tests, CI, local dev) or a
standalone cluster URL such as ``spark://spark-master:7077`` (Docker Compose's ``cluster``
profile). On a cluster, executors connect back to the driver, so ``SPARK_DRIVER_HOST`` must be
a hostname they can resolve and ``SPARK_DRIVER_PORT``/``SPARK_BLOCK_MANAGER_PORT`` pin the
ports they connect to.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# src/insurance_pipeline/config.py -> repo root is two levels up.
_PACKAGE_ROOT = Path(__file__).resolve().parent
_DEFAULT_REPO_ROOT = _PACKAGE_ROOT.parent.parent


SOURCE_MODES = ("files", "databases")


def _env_path(name: str, default: Path) -> str:
    return os.environ.get(name, str(default))


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved locations and connection settings for one pipeline run."""

    data_root: str = field(
        default_factory=lambda: _env_path("DATA_ROOT", _DEFAULT_REPO_ROOT / "data" / "samples")
    )
    schema_root: str = field(
        default_factory=lambda: _env_path("SCHEMA_ROOT", _DEFAULT_REPO_ROOT / "data" / "schemas")
    )
    warehouse_dir: str = field(
        default_factory=lambda: _env_path("WAREHOUSE_DIR", _DEFAULT_REPO_ROOT / "warehouse")
    )

    source_mode: str = field(default_factory=lambda: os.environ.get("SOURCE_MODE", "files"))

    spark_master: str = field(default_factory=lambda: os.environ.get("SPARK_MASTER", "local[*]"))
    spark_driver_host: str = field(default_factory=lambda: os.environ.get("SPARK_DRIVER_HOST", ""))
    spark_driver_port: str = field(default_factory=lambda: os.environ.get("SPARK_DRIVER_PORT", ""))
    spark_block_manager_port: str = field(
        default_factory=lambda: os.environ.get("SPARK_BLOCK_MANAGER_PORT", "")
    )
    spark_executor_memory: str = field(default_factory=lambda: os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))

    mysql_host: str = field(default_factory=lambda: os.environ.get("MYSQL_HOST", "localhost"))
    mysql_port: int = field(default_factory=lambda: int(os.environ.get("MYSQL_PORT", "3306")))
    mysql_user: str = field(default_factory=lambda: os.environ.get("MYSQL_USER", "root"))
    mysql_password: str = field(default_factory=lambda: os.environ.get("MYSQL_PASSWORD", ""))
    mysql_database: str = field(default_factory=lambda: os.environ.get("MYSQL_DATABASE", "insurance"))
    mysql_policies_table: str = field(
        default_factory=lambda: os.environ.get("MYSQL_POLICIES_TABLE", "policies")
    )

    mongo_uri: str = field(default_factory=lambda: os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    mongo_database: str = field(default_factory=lambda: os.environ.get("MONGO_DATABASE", "insurance"))
    mongo_claims_collection: str = field(
        default_factory=lambda: os.environ.get("MONGO_CLAIMS_COLLECTION", "claims")
    )

    clickhouse_host: str = field(default_factory=lambda: os.environ.get("CLICKHOUSE_HOST", "localhost"))
    clickhouse_port: int = field(default_factory=lambda: int(os.environ.get("CLICKHOUSE_PORT", "8123")))
    clickhouse_user: str = field(default_factory=lambda: os.environ.get("CLICKHOUSE_USER", "default"))
    clickhouse_password: str = field(default_factory=lambda: os.environ.get("CLICKHOUSE_PASSWORD", ""))
    clickhouse_database: str = field(
        default_factory=lambda: os.environ.get("CLICKHOUSE_DATABASE", "insurance")
    )

    def __post_init__(self) -> None:
        if self.source_mode not in SOURCE_MODES:
            raise ValueError(f"SOURCE_MODE must be one of {SOURCE_MODES}, got {self.source_mode!r}")

    @property
    def is_local_spark(self) -> bool:
        return self.spark_master.startswith("local")

    @property
    def mysql_jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"

    @property
    def policies_csv_path(self) -> str:
        return str(Path(self.data_root) / "mysql" / "policies.csv")

    @property
    def claims_json_path(self) -> str:
        # The same file docker/mongo/init-mongo.sh seeds the Mongo collection from.
        return str(Path(self.data_root) / "mongodb" / "claims.json")

    @property
    def accidents_csv_path(self) -> str:
        # A local copy of NYC Open Data's Motor Vehicle Collisions extract,
        # standing in for an S3 bucket; read from disk in both source modes.
        return str(Path(self.data_root) / "s3" / "external" / "accidents.csv.gz")

    @property
    def policies_schema_path(self) -> str:
        return str(Path(self.schema_root) / "sql" / "policies.json")

    @property
    def accidents_schema_path(self) -> str:
        return str(Path(self.schema_root) / "s3" / "accidents.json")
