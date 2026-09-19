"""Environment-driven configuration.

Every path/connection detail that used to be hardcoded inline in the Databricks
notebooks (e.g. ``/Workspace/Users/<someone>/insurance-claims/data/...``) lives
here instead, read from environment variables with sensible local defaults so
the same code runs on a laptop, in Docker Compose/Airflow, in CI, and (via env
vars set in a Databricks job/cluster) on Databricks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# src/insurance_pipeline/config.py -> repo root is two levels up.
_PACKAGE_ROOT = Path(__file__).resolve().parent
_DEFAULT_REPO_ROOT = _PACKAGE_ROOT.parent.parent


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

    clickhouse_host: str = field(default_factory=lambda: os.environ.get("CLICKHOUSE_HOST", "localhost"))
    clickhouse_port: int = field(default_factory=lambda: int(os.environ.get("CLICKHOUSE_PORT", "8123")))
    clickhouse_user: str = field(default_factory=lambda: os.environ.get("CLICKHOUSE_USER", "default"))
    clickhouse_password: str = field(default_factory=lambda: os.environ.get("CLICKHOUSE_PASSWORD", ""))
    clickhouse_database: str = field(
        default_factory=lambda: os.environ.get("CLICKHOUSE_DATABASE", "insurance")
    )

    @property
    def policies_csv_path(self) -> str:
        return str(Path(self.data_root) / "mysql" / "policies.csv")

    @property
    def claims_json_path(self) -> str:
        return str(Path(self.data_root) / "s3" / "tmp" / "claims.json")

    @property
    def accidents_csv_path(self) -> str:
        return str(Path(self.data_root) / "s3" / "external" / "accidents.csv.gz")

    @property
    def policies_schema_path(self) -> str:
        return str(Path(self.schema_root) / "sql" / "policies.json")

    @property
    def accidents_schema_path(self) -> str:
        return str(Path(self.schema_root) / "s3" / "accidents.json")
