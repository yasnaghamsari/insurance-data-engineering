"""Write N-times-larger copies of the three sample sources, for benchmarking.

    python scripts/scale_data.py --scale 10            # -> data/benchmark/x10/

The output mirrors ``data/samples`` (``mysql/policies.csv``,
``mongodb/claims.json``, ``s3/external/accidents.csv.gz``), so pointing
``DATA_ROOT`` at it is all the pipeline needs. ``data/samples`` is only ever
read.

Each source is written as N back-to-back copies of the original. Copy 0 is
the original records unchanged; every later copy ``k`` only rewrites its
keys, so they stay unique across copies:

- ``POLICY_NO`` / ``claim_no`` / claims' ``policy_no``: ``"<original>-x<k>"``.
  All three are strings in the source (policy numbers include values like
  ``101610176/1``), so a suffix keeps Spark's inferred type, and a copied
  claim points at the same copy of its policy. An empty ``POLICY_NO`` stays
  empty (Spark reads it as null; Silver drops it).
- ``COLLISION_ID``: ``id + k * 10,000,000``. The largest sample id is ~4.2M,
  so copies never collide, and even x20 stays below 2^31 (still an int).

Everything else is byte-for-byte the same, so Silver's dedup, the
quality gate's uniqueness checks and the claims-to-policies join behave on
copy ``k`` exactly as on the original: additive Gold measures come out at
exactly N times the sample's, ratios and modes unchanged.
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLES = REPO_ROOT / "data" / "samples"
DEFAULT_OUT = REPO_ROOT / "data" / "benchmark"

POLICIES = Path("mysql") / "policies.csv"
CLAIMS = Path("mongodb") / "claims.json"
ACCIDENTS = Path("s3") / "external" / "accidents.csv.gz"

COLLISION_ID_STRIDE = 10_000_000
# Columns after COLLISION_ID in the accidents header (VEHICLE_TYPE_CODE_1..5).
FIELDS_AFTER_COLLISION_ID = 5
INT_MAX = 2**31 - 1


def copy_key(value: str, k: int) -> str:
    """Key of copy ``k``. Copy 0 and empty keys are left as they are."""
    return value if k == 0 or value == "" else f"{value}-x{k}"


def scale_policies(src: Path, dst: Path, scale: int) -> int:
    """Write ``scale`` copies of the policies CSV (header once); returns records per copy.

    Works on raw bytes, line by line, tracking whether a line starts inside a
    quoted field: 22 records have a newline inside a quoted CHASSIS_NO, and
    their continuation lines must be copied as-is rather than mistaken for
    a new record.
    """
    with open(src, "rb") as f:
        header = f.readline()
        body = f.readlines()
    if not header.startswith(b"CUST_ID,POLICY_NO,"):
        raise ValueError(f"Unexpected policies header: {header[:60]!r}")

    with open(dst, "wb") as out:
        out.write(header)
        records = 0
        for k in range(scale):
            in_quotes = False
            for line in body:
                if in_quotes or k == 0:
                    out.write(line)
                else:
                    cust_id, policy_no, rest = line.split(b",", 2)
                    if b'"' in policy_no:
                        raise ValueError(f"Quoted POLICY_NO not supported: {line[:80]!r}")
                    new_key = copy_key(policy_no.decode("utf-8"), k).encode("utf-8")
                    out.write(b",".join((cust_id, new_key, rest)))
                if not in_quotes and k == 0:
                    records += 1
                # An odd number of quote characters flips in/out of a quoted field.
                if line.count(b'"') % 2:
                    in_quotes = not in_quotes
            if in_quotes:
                raise ValueError("policies.csv ends inside a quoted field")
    return records


def scale_claims(src: Path, dst: Path, scale: int) -> int:
    """Write ``scale`` copies of the claims JSON array; returns claims per copy."""
    with open(src, encoding="utf-8") as f:
        claims = json.load(f)
    scaled = []
    for k in range(scale):
        for claim in claims:
            copy = dict(claim)
            copy["claim_no"] = copy_key(claim["claim_no"], k)
            copy["policy_no"] = copy_key(claim["policy_no"], k)
            scaled.append(copy)
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(scaled, f, indent=2, ensure_ascii=False)
    return len(claims)


def _raw_fields(record: bytes) -> list[bytes]:
    """Split one CSV record into its fields' raw bytes (quotes kept), honouring
    commas inside quoted fields."""
    fields, current, in_quotes = [], bytearray(), False
    for byte in record:
        if byte == ord('"'):
            in_quotes = not in_quotes
        if byte == ord(",") and not in_quotes:
            fields.append(bytes(current))
            current.clear()
        else:
            current.append(byte)
    fields.append(bytes(current))
    return fields


def _shift_collision_id(line: bytes, k: int) -> bytes:
    """``line`` with its COLLISION_ID moved into copy ``k``'s id range."""
    record = line.splitlines()[0]
    eol = line[len(record) :]
    # Fast path: no comma inside the trailing VEHICLE_TYPE_CODE fields, so the
    # id is the 6th field from the end. A handful of rows have a quoted
    # comma there; those go through the (slow) quote-aware split.
    fields = record.rsplit(b",", FIELDS_AFTER_COLLISION_ID + 1)
    if b'"' in b"".join(fields[1:]) or len(fields) != FIELDS_AFTER_COLLISION_ID + 2:
        fields = _raw_fields(record)
    index = len(fields) - FIELDS_AFTER_COLLISION_ID - 1
    collision_id = fields[index]
    if collision_id:
        if not collision_id.isdigit():
            raise ValueError(f"Unexpected COLLISION_ID {collision_id!r} in {line[:120]!r}")
        new_value = int(collision_id) + k * COLLISION_ID_STRIDE
        if new_value > INT_MAX:
            raise ValueError("COLLISION_ID would overflow an int; lower the scale factor")
        fields[index] = str(new_value).encode()
    return b",".join(fields) + eol


def scale_accidents(src: Path, dst: Path, scale: int) -> int:
    """Write ``scale`` copies of the gzipped accidents CSV, still a single .gz
    file (so still unsplittable for Spark, like the original); returns rows per copy."""
    rows = 0
    with gzip.open(dst, "wb", compresslevel=6) as out:
        for k in range(scale):
            with gzip.open(src, "rb") as f:
                header = f.readline()
                if k == 0:
                    if b",COLLISION_ID," not in header:
                        raise ValueError("Unexpected accidents header")
                    out.write(header)
                for line in f:
                    out.write(line if k == 0 else _shift_collision_id(line, k))
                    if k == 0:
                        rows += 1
    return rows


def scale_all(scale: int, out_root: Path, samples: Path = SAMPLES, force: bool = False) -> Path:
    if scale < 1:
        raise ValueError("scale must be >= 1")
    if scale * COLLISION_ID_STRIDE > INT_MAX:
        raise ValueError(f"scale {scale} would push COLLISION_ID past 2^31")
    target = out_root / f"x{scale}"
    if target.resolve() == samples.resolve() or samples.resolve() in target.resolve().parents:
        raise ValueError("Refusing to write inside data/samples")
    if target.exists():
        if not force:
            print(f"{target} already exists (use --force to regenerate)")
            return target
        shutil.rmtree(target)

    staging = out_root / f".x{scale}.tmp"
    shutil.rmtree(staging, ignore_errors=True)
    for rel in (POLICIES, CLAIMS, ACCIDENTS):
        (staging / rel).parent.mkdir(parents=True, exist_ok=True)

    for name, func, rel in (
        ("policies", scale_policies, POLICIES),
        ("claims", scale_claims, CLAIMS),
        ("accidents", scale_accidents, ACCIDENTS),
    ):
        started = time.perf_counter()
        per_copy = func(samples / rel, staging / rel, scale)
        size_mb = (staging / rel).stat().st_size / 2**20
        print(
            f"x{scale} {name}: {per_copy:,} records x {scale} = {per_copy * scale:,} "
            f"({size_mb:,.0f} MB, {time.perf_counter() - started:.0f}s)"
        )
    staging.rename(target)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scale", type=int, nargs="+", required=True, help="scale factor(s), e.g. 5 10 20")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--force", action="store_true", help="regenerate even if the folder exists")
    args = parser.parse_args(argv)
    for scale in args.scale:
        scale_all(scale, args.out, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
