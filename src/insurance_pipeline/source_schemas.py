"""Explicit read schemas for the file sources Bronze ingests.

These are exactly the schemas Spark infers from the sample files
(``inferSchema`` for the CSVs, JSON schema inference for claims), written out
so Bronze doesn't have to infer them on every run. Inference is a full extra
pass over the input before the real read: for the gzipped accidents CSV —
which can't be split, so a single task decompresses and parses all of it —
that was ~20% of Bronze's runtime at 5x the sample size (see
docs/benchmark.md).

Types are deliberately left as inferred, including the odd ones (e.g.
``ACCIDENT_DATE``/``ZIP_CODE`` as strings, because a few raw rows don't parse
as timestamps/numbers), so Bronze — and everything downstream of it — comes
out exactly as before. Silver does the real typing.
``tests/test_bronze.py`` checks each one still equals what Spark infers from
the sample file, so a change in the source data fails CI instead of quietly
nulling out values.
"""

from __future__ import annotations

POLICIES_SCHEMA = ", ".join(
    [
        "CUST_ID DOUBLE",
        "POLICY_NO STRING",
        "POLICYTYPE STRING",
        "POL_ISSUE_DATE STRING",
        "POL_EFF_DATE STRING",
        "POL_EXPIRY_DATE STRING",
        "BODY STRING",
        "MAKE STRING",
        "MODEL STRING",
        "MODEL_YEAR DOUBLE",
        "CHASSIS_NO STRING",
        "USE_OF_VEHICLE STRING",
        "DRV_DOB STRING",
        "BOROUGH STRING",
        "NEIGHBORHOOD STRING",
        "ZIP_CODE DOUBLE",
        "PRODUCT STRING",
        "SUM_INSURED DOUBLE",
        "PREMIUM DOUBLE",
        "DEDUCTABLE INT",
    ]
)

# JSON schema inference orders fields alphabetically, nested ones included.
CLAIMS_SCHEMA = ", ".join(
    [
        "claim_amount STRUCT<injury: BIGINT, property: BIGINT, total: BIGINT, vehicle: BIGINT>",
        "claim_datetime STRING",
        "claim_no STRING",
        "collision STRUCT<number_of_vehicles_involved: BIGINT, type: STRING>",
        "driver STRUCT<age: DOUBLE, insured_relationship: STRING, license_issue_date: STRING>",
        "incident STRUCT<date: STRING, hour: BIGINT, severity: STRING, type: STRING>",
        "months_as_customer BIGINT",
        "number_of_witnesses BIGINT",
        "policy_no STRING",
        "suspicious_activity BOOLEAN",
    ]
)

ACCIDENTS_SCHEMA = ", ".join(
    [
        "ACCIDENT_DATE STRING",
        "ACCIDENT_TIME STRING",
        "BOROUGH STRING",
        "ZIP_CODE STRING",
        "LATITUDE DOUBLE",
        "LONGITUDE DOUBLE",
        "ON_STREET_NAME STRING",
        "CROSS_STREET_NAME STRING",
        "OFF_STREET_NAME STRING",
        "NUMBER_OF_PERSONS_INJURED STRING",
        "NUMBER_OF_PERSONS_KILLED STRING",
        "NUMBER_OF_PEDESTRIANS_INJURED INT",
        "NUMBER_OF_PEDESTRIANS_KILLED INT",
        "NUMBER_OF_CYCLIST_INJURED INT",
        "NUMBER_OF_CYCLIST_KILLED INT",
        "NUMBER_OF_MOTORIST_INJURED STRING",
        "NUMBER_OF_MOTORIST_KILLED STRING",
        "CONTRIBUTING_FACTOR_VEHICLE_1 STRING",
        "CONTRIBUTING_FACTOR_VEHICLE_2 STRING",
        "CONTRIBUTING_FACTOR_VEHICLE_3 STRING",
        "CONTRIBUTING_FACTOR_VEHICLE_4 STRING",
        "CONTRIBUTING_FACTOR_VEHICLE_5 STRING",
        "COLLISION_ID INT",
        "VEHICLE_TYPE_CODE_1 STRING",
        "VEHICLE_TYPE_CODE_2 STRING",
        "VEHICLE_TYPE_CODE_3 STRING",
        "VEHICLE_TYPE_CODE_4 STRING",
        "VEHICLE_TYPE_CODE_5 STRING",
    ]
)
