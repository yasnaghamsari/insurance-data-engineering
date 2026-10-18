import json
import sys
import types

import pytest

from insurance_pipeline import bronze
from insurance_pipeline.bronze import ingest_raw_data
from insurance_pipeline.config import PipelineConfig
from insurance_pipeline.spark_utils import MYSQL_JDBC_PACKAGE, extra_spark_packages


def test_run_bronze_writes_all_tables(spark, bronze_tables):
    assert spark.table("bronze_policies").count() > 0
    assert spark.table("bronze_claims").count() > 0
    assert spark.table("bronze_accidents").count() > 0


def test_bronze_tables_have_ingestion_timestamp(spark, bronze_tables):
    for table_name in ("bronze_policies", "bronze_claims", "bronze_accidents"):
        assert "ingestion_timestamp" in spark.table(table_name).columns


def test_ingest_raw_data_rejects_unknown_format(spark):
    try:
        ingest_raw_data(spark, "irrelevant/path.parquet", file_format="parquet")
    except ValueError as exc:
        assert "Unsupported file format" in str(exc)
    else:
        raise AssertionError("expected ValueError for unsupported file format")


def test_bronze_claims_reads_the_same_file_mongo_is_seeded_from(spark, bronze_tables, cfg):
    with open(cfg.claims_json_path, encoding="utf-8") as claims_file:
        expected = len(json.load(claims_file))
    assert spark.table("bronze_claims").count() == expected


def test_bronze_policies_keeps_records_with_a_newline_inside_a_quoted_field(spark, bronze_tables):
    # Policy 102054785's CHASSIS_NO is quoted and ends in a newline. Read
    # line by line, the record would split into two rows, neither with both
    # a policy number and a premium.
    row = spark.table("bronze_policies").filter("POLICY_NO = '102054785'").first()
    assert row is not None
    assert row["PREMIUM"] == 1667.0


def test_source_mode_defaults_to_files_and_rejects_unknown_values(monkeypatch):
    monkeypatch.delenv("SOURCE_MODE", raising=False)
    assert PipelineConfig().source_mode == "files"
    with pytest.raises(ValueError, match="SOURCE_MODE"):
        PipelineConfig(source_mode="s3")


def test_mysql_jdbc_driver_is_only_added_in_databases_mode():
    assert extra_spark_packages(PipelineConfig(source_mode="files")) == []
    assert extra_spark_packages(PipelineConfig(source_mode="databases")) == [MYSQL_JDBC_PACKAGE]


def test_mysql_jdbc_options_come_from_config():
    cfg = PipelineConfig(
        source_mode="databases",
        mysql_host="mysql",
        mysql_port=3307,
        mysql_user="u",
        mysql_password="pw",
        mysql_database="insurance",
    )

    options = bronze.mysql_jdbc_options(cfg)

    assert options["url"] == "jdbc:mysql://mysql:3307/insurance"
    assert options["dbtable"] == "policies"
    assert (options["user"], options["password"]) == ("u", "pw")


def test_fetch_mongo_claims_excludes_id_and_closes_client(monkeypatch):
    calls = {}

    class FakeCollection:
        def find(self, query, projection):
            calls["projection"] = projection
            return [{"claim_no": "c1", "driver": {"age": 30.0}}]

    class FakeClient:
        def __init__(self, uri):
            calls["uri"] = uri

        def __getitem__(self, name):
            return {"claims": FakeCollection()}

        def close(self):
            calls["closed"] = True

    monkeypatch.setitem(sys.modules, "pymongo", types.SimpleNamespace(MongoClient=FakeClient))
    cfg = PipelineConfig(source_mode="databases", mongo_uri="mongodb://mongo:27017")

    documents = bronze.fetch_mongo_claims(cfg)

    assert [json.loads(d) for d in documents] == [{"claim_no": "c1", "driver": {"age": 30.0}}]
    assert calls == {"uri": "mongodb://mongo:27017", "projection": {"_id": 0}, "closed": True}


def test_databases_mode_claims_match_files_mode(spark, cfg, monkeypatch, tmp_path):
    # Mongo is seeded from the same JSON file files mode reads, so the two
    # paths into Bronze must produce identical rows and schema.
    with open(cfg.claims_json_path, encoding="utf-8") as claims_file:
        sample = json.load(claims_file)[:200]
    (tmp_path / "mongodb").mkdir()
    (tmp_path / "mongodb" / "claims.json").write_text(json.dumps(sample), encoding="utf-8")

    monkeypatch.setattr(bronze, "fetch_mongo_claims", lambda _cfg: [json.dumps(doc) for doc in sample])
    from_files = bronze.ingest_claims(spark, PipelineConfig(data_root=str(tmp_path), source_mode="files"))
    from_mongo = bronze.ingest_claims(spark, PipelineConfig(source_mode="databases"))

    from_files = from_files.drop("ingestion_timestamp")
    from_mongo = from_mongo.drop("ingestion_timestamp")
    assert from_mongo.schema == from_files.schema
    assert sorted(from_mongo.collect()) == sorted(from_files.collect())
