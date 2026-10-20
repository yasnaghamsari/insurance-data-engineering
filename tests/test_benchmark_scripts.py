"""Tests for the benchmark tooling in scripts/ (no Spark needed)."""

import csv
import gzip
import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


scale_data = _load("scale_data")
compare_gold = _load("compare_gold")
analyze_event_log = _load("analyze_event_log")


# --- scale_data ---------------------------------------------------------------

POLICIES_CSV = (
    "CUST_ID,POLICY_NO,POLICYTYPE,CHASSIS_NO,PREMIUM\n"
    "749.0,102048936,COMP,4T1BF28B74U386791,1090.0\n"
    '750.0,102054785,TP,"JL7BCE1K\n",1667.0\n'
    "751.0,,TP,ABC,10.0\n"
    "752.0,101610176/1,TP,DEF,20.0\n"
)
CLAIMS = [
    {"claim_no": "aaa", "policy_no": "102048936", "claim_amount": {"total": 1, "injury": 0}},
    {"claim_no": "bbb", "policy_no": "101610176/1", "claim_amount": {"total": 2, "injury": 0.0}},
]
ACCIDENTS_HEADER = (
    "ACCIDENT_DATE,BOROUGH,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,"
    "VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5\n"
)
ACCIDENTS_ROWS = (
    "2019-08-05T00:00:00.000,QUEENS,4184637,Sedan,Pick-up Truck,,,\n"
    '2019-08-06T00:00:00.000,"BROOKLYN, NY",23,"Box, Truck",Sedan,,,\n'
    "2019-08-07T00:00:00.000,,,Sedan,,,,\n"
)


@pytest.fixture()
def samples(tmp_path):
    root = tmp_path / "samples"
    (root / "mysql").mkdir(parents=True)
    (root / "mongodb").mkdir()
    (root / "s3" / "external").mkdir(parents=True)
    (root / "mysql" / "policies.csv").write_bytes(POLICIES_CSV.encode())
    (root / "mongodb" / "claims.json").write_text(json.dumps(CLAIMS, indent=2), encoding="utf-8")
    with gzip.open(root / "s3" / "external" / "accidents.csv.gz", "wb") as f:
        f.write((ACCIDENTS_HEADER + ACCIDENTS_ROWS).encode())
    return root


@pytest.fixture()
def scaled(samples, tmp_path):
    return scale_data.scale_all(3, tmp_path / "out", samples=samples)


def _policy_rows(root: Path) -> list[list[str]]:
    with open(root / "mysql" / "policies.csv", newline="", encoding="utf-8") as f:
        return list(csv.reader(f))[1:]


def test_scaled_policies_keep_copy_zero_and_suffix_later_keys(scaled):
    rows = _policy_rows(scaled)
    assert len(rows) == 3 * 4
    # Copy 0 is the original, multiline record included.
    assert [r[1] for r in rows[:4]] == ["102048936", "102054785", "", "101610176/1"]
    assert rows[1][3] == "JL7BCE1K\n"
    assert [r[1] for r in rows[8:]] == ["102048936-x2", "102054785-x2", "", "101610176/1-x2"]
    non_empty = [r[1] for r in rows if r[1]]
    assert len(non_empty) == len(set(non_empty))
    # Nothing but the key changes.
    assert [r[0] + r[2] + r[3] + r[4] for r in rows[4:8]] == [r[0] + r[2] + r[3] + r[4] for r in rows[:4]]


def test_scaled_claims_point_at_their_own_copy_of_the_policy(scaled):
    claims = json.loads((scaled / "mongodb" / "claims.json").read_text(encoding="utf-8"))
    policy_numbers = {r[1] for r in _policy_rows(scaled) if r[1]}
    assert len(claims) == 6
    assert len({c["claim_no"] for c in claims}) == 6
    assert claims[2]["claim_no"] == "aaa-x1" and claims[2]["policy_no"] == "102048936-x1"
    assert all(c["policy_no"] in policy_numbers for c in claims)
    # Values are untouched, types included (0.0 stays a float).
    assert claims[5]["claim_amount"] == {"total": 2, "injury": 0.0}


def test_scaled_accidents_shift_collision_ids_and_keep_other_fields(scaled):
    with gzip.open(scaled / "s3" / "external" / "accidents.csv.gz", "rt", encoding="utf-8") as f:
        rows = list(csv.reader(io.StringIO(f.read())))
    header, body = rows[0], rows[1:]
    assert len(body) == 9
    ids = [r[header.index("COLLISION_ID")] for r in body]
    stride = scale_data.COLLISION_ID_STRIDE
    assert ids == ["4184637", "23", "", str(4184637 + stride), str(23 + stride), "", *ids[6:]]
    assert ids[7] == str(23 + 2 * stride)
    # The quoted fields with commas survived, in every copy.
    assert {r[1] for r in body[1::3]} == {"BROOKLYN, NY"}
    assert {r[3] for r in body[1::3]} == {"Box, Truck"}


def test_scale_refuses_collision_ids_beyond_int_range(samples, tmp_path):
    with pytest.raises(ValueError, match="2\\^31"):
        scale_data.scale_all(500, tmp_path / "out", samples=samples)


# --- compare_gold ---------------------------------------------------------------


SCHEMA = [["month", "string"], ["claims", "bigint"], ["ratio", "double"]]


def test_compare_table_accepts_identical_and_tiny_float_differences():
    a = [["2020-01", 3, 84.97], ["2020-02", 4, 0.1 + 0.2]]
    b = [["2020-01", 3, 84.97], ["2020-02", 4, 0.3]]
    diff = compare_gold.compare_table(SCHEMA, a, b)
    assert diff["mismatches"] == [] and diff["row_count"] is None
    assert 0 < diff["max_rel_diff"] < 1e-9


def test_compare_table_reports_real_differences():
    a = [["2020-01", 3, 84.97]]
    assert compare_gold.compare_table(SCHEMA, a, [["2020-01", 4, 84.97]])["mismatches"] == [
        (0, "claims", 3, 4)
    ]
    assert compare_gold.compare_table(SCHEMA, a, [["2020-01", 3, 84.98]])["mismatches"]
    assert compare_gold.compare_table(SCHEMA, a, [])["row_count"] == (1, 0)


# --- analyze_event_log ---------------------------------------------------------------


def _event_log(tmp_path: Path) -> Path:
    events = [
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 0,
            "Submission Time": 1000,
            "Stage IDs": [0],
            "Properties": {"spark.job.description": "bronze | read accidents.csv.gz"},
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 0,
            "Task Info": {"Launch Time": 1000, "Finish Time": 5000},
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {
                "Stage ID": 0,
                "Stage Name": "csv at NativeMethodAccessorImpl.java:0",
                "Number of Tasks": 1,
                "Submission Time": 1000,
                "Completion Time": 5000,
                "Accumulables": [
                    {"Name": "internal.metrics.input.bytesRead", "Value": 2 * 2**20},
                    {"Name": "internal.metrics.input.recordsRead", "Value": 1234},
                ],
            },
        },
        {"Event": "SparkListenerJobEnd", "Job ID": 0, "Completion Time": 5100},
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 1,
            "Submission Time": 6000,
            "Stage IDs": [1],
            "Properties": {"spark.job.description": "Delta: bronze | write bronze_accidents: Filtering"},
        },
        {"Event": "SparkListenerJobEnd", "Job ID": 1, "Completion Time": 6500},
    ]
    path = tmp_path / "local-1"
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    return path


def test_event_log_summary_groups_jobs_by_step_and_action(tmp_path):
    jobs, stages, _ = analyze_event_log.parse(_event_log(tmp_path))
    rows = analyze_event_log.summarize_by_description(jobs, stages)
    assert [(r["step"], r["wall_s"], r["tasks"]) for r in rows] == [("bronze", 4.1, 1), ("bronze", 0.5, 0)]
    assert stages[0].max_task_ms == 4000
    assert stages[0].metrics["input_records"] == 1234
    assert "| bronze | 4.6 |" in analyze_event_log.render(_event_log(tmp_path), top=5, show_plans=False)


def test_plan_features_reads_the_final_adaptive_plan():
    plan = """== Physical Plan ==
AdaptiveSparkPlan (9)
+- == Final Plan ==
   Window (4)
   +- * BroadcastHashJoin LeftOuter BuildRight (3)
      :- AQEShuffleRead (2)
      :  +- Exchange (1)
      +- Exchange (5)
+- == Initial Plan ==
   SortMergeJoin LeftOuter (8)


(1) Exchange
Arguments: hashpartitioning(accident_date#1, 200), ENSURE_REQUIREMENTS, [plan_id=1]

(2) AQEShuffleRead
Arguments: coalesced

(5) Exchange
Arguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=2]
"""
    assert analyze_event_log.plan_features(plan) == {
        "Window": 1,
        "BroadcastHashJoin": 1,
        "AQEShuffleRead coalesced": 1,
        "Exchange hashpartitioning(200)": 1,
        "Exchange SinglePartition": 1,
    }
