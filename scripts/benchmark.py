"""Time the pipeline's Spark steps on scaled-up data, locally or on the Docker cluster.

    python scripts/scale_data.py --scale 5 10 20                       # inputs first
    python scripts/benchmark.py run --scales 1 5 10 20 --label baseline
    python scripts/benchmark.py run --scales 10 --mode cluster --label baseline
    python scripts/benchmark.py report                                  # markdown tables

Each run is one fresh process — one SparkSession for all four steps, with
Spark's cache cleared between steps to match Airflow, where each step is its
own process — that runs bronze, silver, quality and gold (not the
ClickHouse load) with ``SOURCE_MODE=files``,
``DATA_ROOT`` pointing at ``data/benchmark/x<N>`` (``data/samples`` for x1)
and its own throwaway ``WAREHOUSE_DIR`` under ``benchmark-runs/``, so the
real ``./warehouse`` is never touched. It records:

- wall-clock seconds for session start-up and for each step;
- every Gold table's row count, plus a few additive totals used to check the
  scaling (claims, accidents and policies issued should be exactly N times
  the x1 numbers) and the overall loss ratio (which shouldn't move at all);
- a Spark event log (``benchmark-runs/events/``), the same data the Spark UI
  and History Server show; ``scripts/analyze_event_log.py`` summarizes it.

Every Spark job is labelled ``"<step> | <action>"`` (e.g. ``"bronze | read
accidents.csv.gz"``, ``"gold | write gold_accidents_daily"``) by wrapping a
few PySpark entry points in this process only, so the event log can be
broken down per table without any change to the pipeline code.

``--src`` runs a different copy of ``src/`` (e.g. the pre-optimization
baseline from ``git archive``) through ``PYTHONPATH``, which takes precedence
over the venv's editable install. ``--mode cluster`` runs the same thing in
the ``benchmark-driver`` container against Docker Compose's ``cluster``
profile (``spark://spark-master:7077``); see
``docker/benchmark/docker-compose.benchmark.yml``.

Results are appended to ``docs/benchmark/results.csv`` (one row per run) and
``docs/benchmark/results.json`` (the same plus per-table Gold counts).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = REPO_ROOT / "benchmark-runs"
RESULTS_DIR = REPO_ROOT / "docs" / "benchmark"
STEPS = ("bronze", "silver", "quality", "gold")

COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(REPO_ROOT / "docker-compose.yml"),
    "-f",
    str(REPO_ROOT / "docker" / "benchmark" / "docker-compose.benchmark.yml"),
    "--profile",
    "cluster",
]
CLUSTER_SERVICES = ["spark-master", "spark-worker-1", "spark-worker-2"]
CSV_FIELDS = [
    "label",
    "mode",
    "scale",
    "repeat",
    "started_at",
    "git_sha",
    "src",
    "spark_master",
    "parallelism",
    "startup_s",
    *(f"{step}_s" for step in STEPS),
    "pipeline_s",
    "gold_tables",
    "gold_rows",
    "claims_total",
    "accidents_total",
    "policies_issued_total",
    "loss_ratio_pct",
    "event_log",
]


# --- One run (runs in its own process: on the host, or in the driver container) ----------


def _instrument(spark, current: dict) -> None:
    """Give every Spark job a ``"<step> | <action>"`` description, for the event log."""
    from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter

    from insurance_pipeline import quality

    def describe(action: str) -> None:
        spark.sparkContext.setJobDescription(f"{current['step']} | {action}")

    def wrap_reader(method_name: str):
        original = getattr(DataFrameReader, method_name)

        def wrapper(self, path, *args, **kwargs):
            describe(f"read {Path(str(path)).name}")
            return original(self, path, *args, **kwargs)

        setattr(DataFrameReader, method_name, wrapper)

    for name in ("csv", "json"):
        wrap_reader(name)

    original_save = DataFrameWriter.saveAsTable

    def save_as_table(self, name, *args, **kwargs):
        describe(f"write {name}")
        return original_save(self, name, *args, **kwargs)

    DataFrameWriter.saveAsTable = save_as_table

    original_first = DataFrame.first

    def first(self):
        describe("first()")
        return original_first(self)

    DataFrame.first = first

    original_validate = quality.validate_dataframe

    def validate_dataframe(spark_, df, table_name, expectations):
        describe(f"validate {table_name}")
        return original_validate(spark_, df, table_name, expectations)

    quality.validate_dataframe = validate_dataframe


def _write_spark_defaults(conf_dir: Path, events_dir: Path) -> None:
    """Event logging is switched on through ``SPARK_CONF_DIR/spark-defaults.conf``,
    which spark-submit reads when PySpark launches the JVM — so the pipeline's
    own session builder doesn't need to know about it."""
    conf_dir.mkdir(parents=True, exist_ok=True)
    events_dir.mkdir(parents=True, exist_ok=True)
    (conf_dir / "spark-defaults.conf").write_text(
        "spark.eventLog.enabled true\n" f"spark.eventLog.dir {events_dir.resolve().as_uri()}\n"
        # Keeps the run logs readable; no effect on the work Spark does.
        "spark.ui.showConsoleProgress false\n",
        encoding="utf-8",
    )


def run_one(args: argparse.Namespace) -> int:
    events_dir = Path(args.events)
    # In the system temp dir rather than next to the events: on Windows,
    # spark-submit's .cmd launcher breaks on a SPARK_CONF_DIR containing a
    # space (e.g. under a "First Last" user folder); %TEMP% is a short path.
    conf_dir = Path(tempfile.mkdtemp(prefix="benchmark-spark-conf-"))
    _write_spark_defaults(conf_dir, events_dir)
    os.environ["SPARK_CONF_DIR"] = str(conf_dir.resolve())
    os.environ["SOURCE_MODE"] = "files"
    os.environ["DATA_ROOT"] = args.data_root
    os.environ["WAREHOUSE_DIR"] = args.warehouse
    # Explicit, because PipelineConfig's default is relative to the package,
    # and --src may point at a copy of src/ outside the repo layout.
    os.environ.setdefault("SCHEMA_ROOT", str(REPO_ROOT / "data" / "schemas"))

    # Imported only now, so PYTHONPATH (--src) and the env vars above apply.
    from pyspark.sql import functions as F

    import insurance_pipeline
    from insurance_pipeline.config import PipelineConfig
    from insurance_pipeline.run import run_step
    from insurance_pipeline.spark_utils import get_local_spark_session

    cfg = PipelineConfig()
    result: dict = {
        "pipeline_module": str(Path(insurance_pipeline.__file__).parent),
        "spark_master": cfg.spark_master,
        "steps": {},
    }

    started = time.perf_counter()
    spark = get_local_spark_session(cfg, app_name=args.app_name)
    result["startup_s"] = round(time.perf_counter() - started, 2)
    if spark.conf.get("spark.eventLog.enabled", "false") != "true":
        raise RuntimeError("Event logging didn't get enabled; check SPARK_CONF_DIR")
    result["app_id"] = spark.sparkContext.applicationId
    result["parallelism"] = spark.sparkContext.defaultParallelism
    result["spark_version"] = spark.version

    current = {"step": "startup"}
    _instrument(spark, current)
    for step in STEPS:
        # In production every step is its own Airflow task, i.e. its own
        # process, so nothing cached by one step (Great Expectations, for
        # one, persists every DataFrame it validates) survives into the
        # next. Clearing the cache here keeps this single-process run honest
        # about that, instead of letting Gold quietly read quality's cache.
        spark.catalog.clearCache()
        current["step"] = step
        spark.sparkContext.setJobDescription(f"{step} | (other)")
        step_started = time.perf_counter()
        run_step(step, spark, cfg)
        result["steps"][step] = round(time.perf_counter() - step_started, 2)
        print(f"  {step}: {result['steps'][step]:.1f}s", flush=True)
    result["pipeline_s"] = round(sum(result["steps"].values()), 2)

    # Not timed: what the run produced, for the scaling sanity check.
    current["step"] = "post"
    spark.sparkContext.setJobDescription("post | gold row counts")
    gold_tables = sorted(t.name for t in spark.catalog.listTables() if t.name.startswith("gold_"))
    result["gold_counts"] = {name: spark.table(name).count() for name in gold_tables}

    def total(table: str, column: str):
        return spark.table(table).agg(F.sum(column)).first()[0]

    result["claims_total"] = total("gold_claims_daily", "number_of_claims")
    result["accidents_total"] = total("gold_accidents_daily", "number_of_accidents")
    result["policies_issued_total"] = total("gold_policies_monthly", "policies_issued")
    loss = spark.table("gold_loss_ratio_monthly").agg(
        F.sum("total_claim_amount").alias("claims"), F.sum("total_premium").alias("premium")
    )
    claims, premium = loss.first()
    result["loss_ratio_pct"] = round(claims / premium * 100, 2) if premium else None

    spark.stop()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2), encoding="utf-8")
    return 0


# --- Orchestration (on the host) --------------------------------------------------------


def _git_sha() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "src"], cwd=REPO_ROOT, capture_output=True, text=True
        )
        return sha + ("+dirty-src" if dirty.stdout.strip() else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _data_root(scale: int) -> Path:
    return REPO_ROOT / "data" / ("samples" if scale == 1 else f"benchmark/x{scale}")


def _compose(*args: str, env: dict, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*COMPOSE, *args], cwd=REPO_ROOT, env=env, check=check, capture_output=capture, text=True
    )


def _relative_to_repo(path: Path) -> str:
    """Compose bind-mount paths are relative to the repo root (posix-style)."""
    try:
        return "./" + path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def _cluster_env(src: Path) -> dict:
    env = dict(os.environ)
    env["PIPELINE_SRC"] = _relative_to_repo(src)
    return env


def _prepare_cluster(scale: int, env: dict) -> None:
    _compose("up", "-d", *CLUSTER_SERVICES, env=env)
    source = "/opt/airflow/data/samples" if scale == 1 else f"/opt/airflow/data/benchmark/x{scale}"
    # The named volumes start out root-owned; the image runs as airflow (uid 50000).
    script = (
        f"test -d /benchmark-data/x{scale} || cp -r {source} /benchmark-data/x{scale}; "
        "chown -R 50000:0 /benchmark-data /benchmark-warehouse"
    )
    _compose(
        "run",
        "--rm",
        "--no-deps",
        "--user",
        "root",
        "--entrypoint",
        "sh",
        "benchmark-driver",
        "-c",
        script,
        env=env,
    )


def _run_with_retry(command: list[str], env: dict, scratch: list[Path], attempts: int = 2) -> None:
    """Run one benchmark process, retrying once if it fails outright.

    On a memory-starved laptop the JVM occasionally dies while starting up
    (py4j "connection reset" before any step has run). That's not a result
    worth recording, so it gets one more go after a pause; a second failure
    is a real error. ``scratch`` (the run's warehouse and event-log dirs)
    is wiped first, so the retry starts as clean as the first attempt.
    """
    for attempt in range(1, attempts + 1):
        try:
            subprocess.run(command, env=env, check=True)
            return
        except subprocess.CalledProcessError:
            if attempt == attempts:
                raise
            print(f"  run failed (attempt {attempt}); retrying in 60s", flush=True)
            for path in scratch:
                shutil.rmtree(path, ignore_errors=True)
            time.sleep(60)


def _run(label: str, mode: str, scale: int, repeat: int, src: Path, keep_warehouse: bool) -> dict:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{label}-{mode}-x{scale}-r{repeat}-{stamp}"
    app_name = f"benchmark {label} {mode} x{scale} r{repeat}"
    result_file = RUNS_DIR / "results" / f"{run_id}.json"
    events_dir = RUNS_DIR / "events" / run_id
    print(f"[{run_id}]", flush=True)

    if mode == "local":
        warehouse_root = RUNS_DIR / "warehouse" / run_id
        env = dict(os.environ)
        env["PYTHONPATH"] = str(src.resolve())
        env["SPARK_MASTER"] = "local[*]"
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "run-one",
            "--data-root",
            str(_data_root(scale)),
            "--warehouse",
            str(warehouse_root / "warehouse"),
            "--events",
            str(events_dir),
            "--out",
            str(result_file),
            "--app-name",
            app_name,
        ]
        _run_with_retry(command, env, scratch=[warehouse_root, events_dir])
        if not keep_warehouse:
            shutil.rmtree(warehouse_root, ignore_errors=True)
    else:
        env = _cluster_env(src)
        _prepare_cluster(scale, env)
        warehouse_root = f"/benchmark-warehouse/{run_id}"
        _compose(
            "run",
            "--rm",
            "--no-deps",
            "benchmark-driver",
            "/opt/benchmark/scripts/benchmark.py",
            "run-one",
            "--data-root",
            f"/benchmark-data/x{scale}",
            "--warehouse",
            f"{warehouse_root}/warehouse",
            "--events",
            f"/opt/benchmark/runs/events/{run_id}",
            "--out",
            f"/opt/benchmark/runs/results/{run_id}.json",
            "--app-name",
            app_name,
            env=env,
        )
        if not keep_warehouse:
            _compose(
                "run",
                "--rm",
                "--no-deps",
                "--entrypoint",
                "rm",
                "benchmark-driver",
                "-rf",
                warehouse_root,
                env=env,
            )

    result = json.loads(result_file.read_text(encoding="utf-8"))
    event_logs = [p for p in events_dir.iterdir() if not p.name.endswith(".inprogress")]
    result.update(
        {
            "label": label,
            "mode": mode,
            "scale": scale,
            "repeat": repeat,
            "started_at": stamp,
            "git_sha": _git_sha(),
            "src": _relative_to_repo(src),
            "event_log": _relative_to_repo(event_logs[0]) if event_logs else "",
            "host": {"os": platform.platform(), "logical_cpus": os.cpu_count()},
        }
    )
    return result


def _flatten(result: dict) -> dict:
    row = {key: result.get(key, "") for key in CSV_FIELDS}
    for step in STEPS:
        row[f"{step}_s"] = result["steps"][step]
    row["gold_tables"] = len(result["gold_counts"])
    row["gold_rows"] = sum(result["gold_counts"].values())
    return row


def _append_results(result: dict) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS_DIR / "results.csv"
    new_file = not csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(_flatten(result))

    json_path = RESULTS_DIR / "results.json"
    existing = json.loads(json_path.read_text(encoding="utf-8")) if json_path.exists() else []
    existing.append(result)
    json_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")


def _check_scaling(result: dict, results: list[dict]) -> None:
    """Additive Gold totals must be exactly N times the same code's x1 totals."""
    reference = next(
        (
            r
            for r in results
            if r["scale"] == 1 and r["mode"] == result["mode"] and r["label"] == result["label"]
        ),
        None,
    )
    if reference is None or result["scale"] == 1:
        return
    n = result["scale"]
    for key in ("claims_total", "accidents_total", "policies_issued_total"):
        if result[key] != n * reference[key]:
            print(f"  WARNING: {key} = {result[key]}, expected {n} x {reference[key]}", flush=True)
    if result["loss_ratio_pct"] != reference["loss_ratio_pct"]:
        print(
            f"  WARNING: loss ratio {result['loss_ratio_pct']} != x1's {reference['loss_ratio_pct']}",
            flush=True,
        )


def run(args: argparse.Namespace) -> int:
    src = Path(args.src)
    if not (src / "insurance_pipeline").is_dir():
        raise SystemExit(f"--src {src} has no insurance_pipeline package")
    for scale in args.scales:
        if not _data_root(scale).is_dir():
            raise SystemExit(f"{_data_root(scale)} is missing; run scripts/scale_data.py --scale {scale}")

    json_path = RESULTS_DIR / "results.json"
    previous = json.loads(json_path.read_text(encoding="utf-8")) if json_path.exists() else []
    for scale in args.scales:
        for repeat in range(1, args.repeat + 1):
            result = _run(args.label, args.mode, scale, repeat, src, args.keep_warehouse)
            _check_scaling(result, previous)
            if args.record:
                _append_results(result)
            previous.append(result)
            steps = " ".join(f"{s}={result['steps'][s]:.1f}s" for s in STEPS)
            print(
                f"  => x{scale} {args.mode} {args.label}: {steps} total={result['pipeline_s']:.1f}s",
                flush=True,
            )
    return 0


# --- Report -----------------------------------------------------------------------------


def report(args: argparse.Namespace) -> int:
    """Median seconds per (label, mode, scale), one markdown table per mode."""
    results = json.loads((RESULTS_DIR / "results.json").read_text(encoding="utf-8"))
    if args.labels:
        results = [r for r in results if r["label"] in args.labels]
    groups: dict[tuple, list[dict]] = {}
    for r in results:
        groups.setdefault((r["mode"], r["label"], r["scale"]), []).append(r)

    for mode in sorted({key[0] for key in groups}):
        print(f"\n### {mode}\n")
        print("| label | scale | runs | startup | " + " | ".join(STEPS) + " | pipeline total |")
        print("|---|---:|---:|---:|" + "---:|" * len(STEPS) + "---:|")
        for (m, label, scale), runs in sorted(groups.items(), key=lambda kv: (kv[0][2], kv[0][1])):
            if m != mode:
                continue
            med = statistics.median
            cells = [f"{med(r['steps'][s] for r in runs):.1f}" for s in STEPS]
            print(
                f"| {label} | x{scale} | {len(runs)} | {med(r['startup_s'] for r in runs):.1f} | "
                + " | ".join(cells)
                + f" | {med(r['pipeline_s'] for r in runs):.1f} |"
            )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="benchmark one or more scale factors")
    p_run.add_argument("--scales", type=int, nargs="+", default=[1, 5, 10, 20])
    p_run.add_argument("--mode", choices=("local", "cluster"), default="local")
    p_run.add_argument("--label", required=True, help="e.g. baseline, explicit-schema")
    p_run.add_argument("--repeat", type=int, default=1)
    p_run.add_argument("--src", default=str(REPO_ROOT / "src"), help="pipeline src/ directory to run")
    p_run.add_argument("--keep-warehouse", action="store_true")
    p_run.add_argument(
        "--no-record", dest="record", action="store_false", help="don't append to docs/benchmark/results.*"
    )
    p_run.set_defaults(func=run)

    p_one = sub.add_parser("run-one", help=argparse.SUPPRESS)
    p_one.add_argument("--data-root", required=True)
    p_one.add_argument("--warehouse", required=True)
    p_one.add_argument("--events", required=True)
    p_one.add_argument("--out", required=True)
    p_one.add_argument("--app-name", default="benchmark")
    p_one.set_defaults(func=run_one)

    p_report = sub.add_parser("report", help="markdown tables of median timings")
    p_report.add_argument("--labels", nargs="*")
    p_report.set_defaults(func=report)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
