"""Check that a code change leaves every Gold table exactly as it was.

    python scripts/compare_gold.py snapshot --warehouse <dir> --out benchmark-runs/gold-snapshots/baseline
    python scripts/compare_gold.py compare benchmark-runs/gold-snapshots/baseline <other snapshot>

``snapshot`` reads each ``gold_*`` Delta table under a warehouse directory
(by path, so it needs no metastore), sorts it by every column, and writes it
as canonical JSON lines plus its schema. ``compare`` then checks two
snapshots table by table: same tables, same schema, same row count, and the
same values row by row — exactly for everything except floating-point
columns, which may differ in the last bits when Spark sums doubles in a
different order (e.g. after a repartition), so those are compared to a
relative tolerance and the largest difference is reported.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

REL_TOL = 1e-9


def _canonical(value):
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


def snapshot(warehouse: Path, out: Path) -> int:
    from insurance_pipeline.config import PipelineConfig
    from insurance_pipeline.spark_utils import get_local_spark_session

    scratch = Path(tempfile.mkdtemp(prefix="compare_gold_"))
    spark = get_local_spark_session(
        PipelineConfig(warehouse_dir=str(scratch / "warehouse"), spark_master="local[*]"),
        app_name="compare-gold",
    )
    tables = sorted(p for p in warehouse.iterdir() if p.name.startswith("gold_") and p.is_dir())
    if not tables:
        raise SystemExit(f"No gold_* tables under {warehouse}")
    out.mkdir(parents=True, exist_ok=True)
    schemas = {}
    for path in tables:
        df = spark.read.format("delta").load(str(path))
        schemas[path.name] = [[f.name, f.dataType.simpleString(), f.nullable] for f in df.schema.fields]
        rows = df.orderBy(*[df[c].asc_nulls_first() for c in df.columns]).collect()
        with open(out / f"{path.name}.jsonl", "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps([_canonical(v) for v in row]) + "\n")
        print(f"{path.name}: {len(rows)} rows")
    (out / "schemas.json").write_text(json.dumps(schemas, indent=2), encoding="utf-8")
    spark.stop()
    return 0


def _load(snapshot_dir: Path) -> tuple[dict, dict]:
    schemas = json.loads((snapshot_dir / "schemas.json").read_text(encoding="utf-8"))
    rows = {}
    for table in schemas:
        with open(snapshot_dir / f"{table}.jsonl", encoding="utf-8") as f:
            rows[table] = [json.loads(line) for line in f]
    return schemas, rows


def compare_table(schema: list, rows_a: list, rows_b: list, rel_tol: float = REL_TOL) -> dict:
    """Differences between two sorted row lists of the same table.

    Returns ``{"row_count": (a, b) or None, "mismatches": [...], "max_rel_diff": float}``.
    """
    float_cols = {i for i, (_, dtype, *_) in enumerate(schema) if dtype in ("double", "float")}
    result = {"row_count": None, "mismatches": [], "max_rel_diff": 0.0}
    if len(rows_a) != len(rows_b):
        result["row_count"] = (len(rows_a), len(rows_b))
        return result
    for row_index, (a, b) in enumerate(zip(rows_a, rows_b, strict=True)):
        for col, (x, y) in enumerate(zip(a, b, strict=True)):
            if x == y:
                continue
            if col in float_cols and isinstance(x, int | float) and isinstance(y, int | float):
                diff = abs(x - y) / max(abs(x), abs(y))
                result["max_rel_diff"] = max(result["max_rel_diff"], diff)
                if diff <= rel_tol:
                    continue
            result["mismatches"].append((row_index, schema[col][0], x, y))
    return result


def compare(dir_a: Path, dir_b: Path, rel_tol: float = REL_TOL) -> int:
    schemas_a, rows_a = _load(dir_a)
    schemas_b, rows_b = _load(dir_b)
    ok = True
    if set(schemas_a) != set(schemas_b):
        print(
            f"Different tables: only in A {set(schemas_a) - set(schemas_b)}, "
            f"only in B {set(schemas_b) - set(schemas_a)}"
        )
        ok = False
    for table in sorted(set(schemas_a) & set(schemas_b)):
        if schemas_a[table] != schemas_b[table]:
            print(f"{table}: schema differs\n  A {schemas_a[table]}\n  B {schemas_b[table]}")
            ok = False
            continue
        diff = compare_table(schemas_a[table], rows_a[table], rows_b[table], rel_tol)
        if diff["row_count"]:
            print(f"{table}: row count differs {diff['row_count']}")
            ok = False
        elif diff["mismatches"]:
            print(f"{table}: {len(diff['mismatches'])} value(s) differ, e.g. {diff['mismatches'][:3]}")
            ok = False
        else:
            exact = (
                "exact"
                if diff["max_rel_diff"] == 0
                else f"max relative float diff {diff['max_rel_diff']:.1e}"
            )
            print(f"{table}: identical ({len(rows_a[table])} rows, {exact})")
    print("ALL GOLD TABLES IDENTICAL" if ok else "GOLD TABLES DIFFER")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_snap = sub.add_parser("snapshot")
    p_snap.add_argument("--warehouse", type=Path, required=True)
    p_snap.add_argument("--out", type=Path, required=True)
    p_cmp = sub.add_parser("compare")
    p_cmp.add_argument("a", type=Path)
    p_cmp.add_argument("b", type=Path)
    p_cmp.add_argument("--rel-tol", type=float, default=REL_TOL)
    args = parser.parse_args(argv)
    if args.command == "snapshot":
        return snapshot(args.warehouse, args.out)
    return compare(args.a, args.b, args.rel_tol)


if __name__ == "__main__":
    sys.exit(main())
