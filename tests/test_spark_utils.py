from insurance_pipeline.config import PipelineConfig
from insurance_pipeline.spark_utils import cluster_spark_conf


def test_spark_master_defaults_to_local_mode(monkeypatch):
    monkeypatch.delenv("SPARK_MASTER", raising=False)
    cfg = PipelineConfig()
    assert cfg.spark_master == "local[*]"
    assert cfg.is_local_spark


def test_spark_master_is_read_from_env(monkeypatch):
    monkeypatch.setenv("SPARK_MASTER", "spark://spark-master:7077")
    cfg = PipelineConfig()
    assert cfg.spark_master == "spark://spark-master:7077"
    assert not cfg.is_local_spark


def test_test_session_runs_in_local_mode(spark):
    assert spark.sparkContext.master.startswith("local")


def test_cluster_spark_conf_is_empty_in_local_mode():
    cfg = PipelineConfig(spark_master="local[2]", spark_driver_host="airflow-scheduler")
    assert cluster_spark_conf(cfg) == {}


def test_cluster_spark_conf_lets_executors_reach_the_driver():
    cfg = PipelineConfig(
        spark_master="spark://spark-master:7077",
        spark_driver_host="airflow-scheduler",
        spark_driver_port="7078",
        spark_block_manager_port="7079",
        spark_executor_memory="1g",
    )
    assert cluster_spark_conf(cfg) == {
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.host": "airflow-scheduler",
        "spark.driver.port": "7078",
        "spark.blockManager.port": "7079",
        "spark.executor.memory": "1g",
    }


def test_cluster_spark_conf_leaves_unset_ports_to_spark():
    cfg = PipelineConfig(spark_master="spark://spark-master:7077", spark_driver_host="", spark_driver_port="")
    conf = cluster_spark_conf(cfg)
    assert "spark.driver.host" not in conf
    assert "spark.driver.port" not in conf
    assert conf["spark.driver.bindAddress"] == "0.0.0.0"
