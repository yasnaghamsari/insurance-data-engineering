from insurance_pipeline.bronze import ingest_raw_data


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
