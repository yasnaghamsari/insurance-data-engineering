"""Summarize a Spark event log: where did the time go?

    python scripts/analyze_event_log.py benchmark-runs/events/<run>/<app-id> [--top 15] [--plans]

An event log is the JSON-lines record Spark writes when
``spark.eventLog.enabled`` is on — the same data the Spark UI / History
Server renders. ``scripts/benchmark.py`` turns it on and labels every job
``"<step> | <action>"``, which this script groups by. It prints markdown:

1. **Per step and per action**: wall time of the jobs it ran (first job
   submitted to last job completed), number of jobs, stages and tasks.
2. **Slowest stages**: duration, task count, the longest single task
   (a stage with 1 task, or one task as long as the stage, didn't
   parallelize), input size/records, shuffle read/write, spill and GC.
3. With ``--plans``: per SQL execution, the physical-plan operators that
   matter here — the join strategy (BroadcastHashJoin vs SortMergeJoin),
   shuffles (``Exchange``) and single-partition exchanges (a window or sort
   with no ``partitionBy``), and AQE's coalesced shuffle reads — taken
   from the final adaptive plan when AQE re-planned the query.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

MB = 2**20


@dataclass
class Stage:
    stage_id: int
    name: str = ""
    num_tasks: int = 0
    submitted: int | None = None
    completed: int | None = None
    metrics: dict = field(default_factory=dict)
    max_task_ms: int = 0
    description: str = ""

    @property
    def duration_ms(self) -> int:
        if self.submitted is None or self.completed is None:
            return 0
        return self.completed - self.submitted


@dataclass
class Job:
    job_id: int
    description: str
    submitted: int
    stage_ids: list
    completed: int | None = None
    sql_execution_id: int | None = None


def _metric(accumulables: list, name: str) -> int:
    for acc in accumulables:
        if acc.get("Name") == name:
            try:
                return int(acc.get("Value", 0))
            except (TypeError, ValueError):
                return 0
    return 0


STAGE_METRICS = {
    "run_ms": "internal.metrics.executorRunTime",
    "gc_ms": "internal.metrics.jvmGCTime",
    "input_bytes": "internal.metrics.input.bytesRead",
    "input_records": "internal.metrics.input.recordsRead",
    "output_bytes": "internal.metrics.output.bytesWritten",
    "shuffle_read_remote": "internal.metrics.shuffle.read.remoteBytesRead",
    "shuffle_read_local": "internal.metrics.shuffle.read.localBytesRead",
    "shuffle_write_bytes": "internal.metrics.shuffle.write.bytesWritten",
    "memory_spill": "internal.metrics.memoryBytesSpilled",
    "disk_spill": "internal.metrics.diskBytesSpilled",
}


def parse(path: Path) -> tuple[dict, dict, dict]:
    """``(jobs, stages, sql_plans)`` from an event log file."""
    jobs: dict[int, Job] = {}
    stages: dict[int, Stage] = {}
    plans: dict[int, dict] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            event = json.loads(line)
            kind = event.get("Event")
            if kind == "SparkListenerJobStart":
                props = event.get("Properties") or {}
                sql_id = props.get("spark.sql.execution.id")
                job = Job(
                    job_id=event["Job ID"],
                    description=props.get("spark.job.description") or "(no description)",
                    submitted=event["Submission Time"],
                    stage_ids=event.get("Stage IDs", []),
                    sql_execution_id=int(sql_id) if sql_id is not None else None,
                )
                jobs[job.job_id] = job
                for stage_id in job.stage_ids:
                    stages.setdefault(stage_id, Stage(stage_id)).description = job.description
            elif kind == "SparkListenerJobEnd":
                if event["Job ID"] in jobs:
                    jobs[event["Job ID"]].completed = event["Completion Time"]
            elif kind == "SparkListenerStageCompleted":
                info = event["Stage Info"]
                stage = stages.setdefault(info["Stage ID"], Stage(info["Stage ID"]))
                stage.name = info.get("Stage Name", "")
                stage.num_tasks = info.get("Number of Tasks", 0)
                stage.submitted = info.get("Submission Time")
                stage.completed = info.get("Completion Time")
                accumulables = info.get("Accumulables", [])
                stage.metrics = {key: _metric(accumulables, name) for key, name in STAGE_METRICS.items()}
            elif kind == "SparkListenerTaskEnd":
                task = event.get("Task Info", {})
                stage = stages.setdefault(event["Stage ID"], Stage(event["Stage ID"]))
                duration = task.get("Finish Time", 0) - task.get("Launch Time", 0)
                stage.max_task_ms = max(stage.max_task_ms, duration)
            elif kind == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                plans[event["executionId"]] = {
                    "description": event.get("description", ""),
                    "plan": event.get("physicalPlanDescription", ""),
                }
            elif kind == "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate":
                if event["executionId"] in plans:
                    # Later updates supersede earlier ones; the last is the final plan.
                    plans[event["executionId"]]["plan"] = event.get("physicalPlanDescription", "")
    return jobs, stages, plans


def _step_of(description: str) -> str:
    """``"bronze"`` for ``"bronze | read x"`` and for Delta's internal jobs,
    which Delta labels ``"Delta: <the running job's description>: ..."``."""
    while description.startswith("Delta: "):
        description = description[len("Delta: ") :]
    return description.split(" | ")[0]


def summarize_by_description(jobs: dict, stages: dict) -> list[dict]:
    """One row per job description (``"<step> | <action>"``), in run order."""
    groups: dict[str, list[Job]] = defaultdict(list)
    for job in sorted(jobs.values(), key=lambda j: j.submitted):
        groups[job.description].append(job)
    rows = []
    for description, group in groups.items():
        completed = [j.completed for j in group if j.completed is not None]
        stage_list = [stages[s] for j in group for s in j.stage_ids if s in stages and stages[s].completed]
        rows.append(
            {
                "description": description,
                "step": _step_of(description),
                "start": min(j.submitted for j in group),
                "wall_s": ((max(completed) - min(j.submitted for j in group)) / 1000) if completed else 0.0,
                "jobs": len(group),
                "stages": len(stage_list),
                "tasks": sum(s.num_tasks for s in stage_list),
                "input_mb": sum(s.metrics.get("input_bytes", 0) for s in stage_list) / MB,
                "shuffle_write_mb": sum(s.metrics.get("shuffle_write_bytes", 0) for s in stage_list) / MB,
            }
        )
    return sorted(rows, key=lambda r: r["start"])


JOINS = ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin")
TREE_OPERATOR = re.compile(r"(?:\+-|:-|^)\s*\*?\s*([A-Za-z]+)(?: [A-Za-z]+)*? \((\d+)\)", re.MULTILINE)
DETAIL_HEADER = re.compile(r"^\((\d+)\) ([A-Za-z]+)", re.MULTILINE)


def plan_features(plan: str) -> dict[str, int]:
    """Operator counts for one SQL execution's physical plan.

    Plans are in EXPLAIN FORMATTED style: an operator tree with numbered
    nodes, then one numbered details block per node. When AQE re-planned the
    query, only the ``== Final Plan ==`` tree is used (what actually ran).
    Exchanges are broken down by partitioning — ``hashpartitioning(..., N)``
    is a regular shuffle into N partitions, ``SinglePartition`` moves every
    row into one task (a window or sort without ``partitionBy``).
    """
    tree = plan.split("\n\n(", 1)[0]
    if "== Final Plan ==" in tree:
        tree = tree.split("== Final Plan ==", 1)[1].split("== Initial Plan ==", 1)[0]
    nodes = {int(node_id): name for name, node_id in TREE_OPERATOR.findall(tree)}

    details: dict[int, str] = {}
    headers = list(DETAIL_HEADER.finditer(plan))
    for i, match in enumerate(headers):
        stop = headers[i + 1].start() if i + 1 < len(headers) else len(plan)
        details[int(match.group(1))] = plan[match.start() : stop]

    features: dict[str, int] = defaultdict(int)
    for node_id, name in nodes.items():
        text = details.get(node_id, "")
        if name in JOINS or name in ("Window", "InMemoryTableScan"):
            features[name] += 1
        elif name == "Exchange":
            args = re.search(r"Arguments: (\w+)(?:\(.*?, (\d+)\))?", text)
            kind = args.group(1) if args else "?"
            partitions = f"({args.group(2)})" if args and args.group(2) else ""
            features[f"Exchange {kind}{partitions}"] += 1
        elif name == "AQEShuffleRead" and "coalesced" in text:
            features["AQEShuffleRead coalesced"] += 1
        elif name == "Scan":
            fmt = re.search(r"^\(\d+\) Scan (\w+)", text)
            features[f"Scan {fmt.group(1) if fmt else '?'}"] += 1
    return dict(features)


def _fmt_mb(value: float) -> str:
    return f"{value:,.0f}" if value >= 10 else f"{value:,.1f}"


def render(path: Path, top: int, show_plans: bool) -> str:
    jobs, stages, plans = parse(path)
    out = [f"## Event log `{path.name}`", ""]

    by_desc = summarize_by_description(jobs, stages)
    step_totals: dict[str, float] = defaultdict(float)
    for row in by_desc:
        step_totals[row["step"]] += row["wall_s"]
    out += ["### Job wall time per step", "", "| step | job wall time (s) |", "|---|---:|"]
    out += [f"| {step} | {secs:.1f} |" for step, secs in step_totals.items()]

    out += [
        "",
        "### Per action",
        "",
        "| step \\| action | wall (s) | jobs | stages | tasks | input MB | shuffle write MB |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in by_desc:
        out.append(
            f"| {row['description'].replace('|', '&#124;')} | {row['wall_s']:.1f} | {row['jobs']} | "
            f"{row['stages']} | {row['tasks']} | {_fmt_mb(row['input_mb'])} | "
            f"{_fmt_mb(row['shuffle_write_mb'])} |"
        )

    completed = [s for s in stages.values() if s.completed is not None]
    slowest = sorted(completed, key=lambda s: s.duration_ms, reverse=True)[:top]
    out += [
        "",
        f"### {top} slowest stages",
        "",
        "| stage | step \\| action | duration (s) | tasks | longest task (s) | input MB | input records |"
        " shuffle read MB | shuffle write MB | spill MB | GC (s) |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s in slowest:
        m = s.metrics
        shuffle_read = (m.get("shuffle_read_remote", 0) + m.get("shuffle_read_local", 0)) / MB
        spill = (m.get("memory_spill", 0) + m.get("disk_spill", 0)) / MB
        out.append(
            f"| {s.stage_id} | {s.description.replace('|', '&#124;')} | {s.duration_ms / 1000:.1f} | "
            f"{s.num_tasks} | {s.max_task_ms / 1000:.1f} | {_fmt_mb(m.get('input_bytes', 0) / MB)} | "
            f"{m.get('input_records', 0):,} | {_fmt_mb(shuffle_read)} | "
            f"{_fmt_mb(m.get('shuffle_write_bytes', 0) / MB)} | {_fmt_mb(spill)} | "
            f"{m.get('gc_ms', 0) / 1000:.1f} |"
        )

    if show_plans:
        description_by_sql = {
            j.sql_execution_id: j.description for j in jobs.values() if j.sql_execution_id is not None
        }
        out += ["", "### Physical-plan operators per SQL execution", ""]
        out += ["| sql id | step \\| action | operators |", "|---:|---|---|"]
        for sql_id, info in sorted(plans.items()):
            label = description_by_sql.get(sql_id, info["description"][:60])
            features = plan_features(info["plan"])
            if not features or label.startswith("Delta: ") or label.startswith("post |"):
                continue
            ops = ", ".join(f"{name} x{count}" for name, count in features.items())
            out.append(f"| {sql_id} | {label.replace('|', '&#124;')} | {ops} |")
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("event_log", type=Path, help="event log file (or a directory holding one)")
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--plans", action="store_true")
    parser.add_argument("--out", type=Path, help="also write the markdown here")
    args = parser.parse_args(argv)

    path = args.event_log
    if path.is_dir():
        logs = [p for p in path.iterdir() if p.is_file() and not p.name.endswith(".inprogress")]
        if len(logs) != 1:
            raise SystemExit(f"Expected one event log in {path}, found {[p.name for p in logs]}")
        path = logs[0]
    text = render(path, args.top, args.plans)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
