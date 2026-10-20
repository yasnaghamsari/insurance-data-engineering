"""Gold layer: business-ready aggregations.

Daily/weekly/monthly claims and accidents aggregates, monthly policies, and
the claims-to-policies joins behind ``gold_loss_ratio_monthly``,
``gold_vehicle_body_risk``, and ``gold_vehicle_usage_risk`` (incurred claims
vs. *written* premium — premium counted in full in the month the policy was
issued, not earned over its term — attributed to the policy, and vehicle,
each claim was filed against).

Every time-series table follows the same shape: groupBy -> agg -> window-based
trend columns (``lag`` for pct-change, a calendar-day ``rangeBetween`` window
for the daily 30-day averages, ``rowsBetween`` for the weekly/monthly ones).
``accident_hour`` is derived in Silver, not here.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from insurance_pipeline.config import PipelineConfig

logger = logging.getLogger(__name__)


def _year_month(date_col: str) -> F.Column:
    return F.concat_ws("-", F.year(date_col), F.lpad(F.month(date_col), 2, "0"))


def _year_week(date_col: str) -> F.Column:
    """ISO 8601 week label, e.g. ``2015-53``.

    ``weekofyear`` is the ISO week number, so it has to be paired with the
    ISO week-based year rather than the calendar ``year``: otherwise
    2016-01-01 (ISO week 53 of 2015) is labelled "2016-53" and sorts after
    the rest of 2016, and 2018-12-31 (ISO week 1 of 2019) is labelled
    "2018-01" and merged into the first week of 2018.
    """
    return F.concat_ws(
        "-",
        F.expr(f"extract(YEAROFWEEK FROM {date_col})"),
        F.lpad(F.weekofyear(date_col), 2, "0"),
    )


def _pct_change(count_col: str, window: Window) -> F.Column:
    current = F.col(count_col)
    previous = F.lag(count_col).over(window)
    return F.round(((current - previous) / previous) * 100, 2)


def _rolling_daily_avg(value_col: str, date_col: str, days: int = 30) -> F.Column:
    """Average of ``value_col`` per calendar day over the ``days`` days ending
    on each row's ``date_col`` (inclusive).

    The daily tables only have a row for days with at least one claim or
    accident, so a row-based window (``rowsBetween``) would span however
    many calendar days those rows happen to cover. This uses a range window
    over a day number instead, and divides the window's total by the number
    of calendar days in it, so a day with no rows counts as zero rather
    than being skipped. At the very start of the series, where fewer than
    ``days`` days exist, it divides by the days elapsed so far.
    """
    day_number = F.unix_date(F.to_date(date_col))
    window = Window.orderBy(day_number).rangeBetween(-(days - 1), 0)
    first_day = F.min(day_number).over(Window.partitionBy())
    days_in_window = F.least(F.lit(days), day_number - first_day + 1)
    return F.round(F.sum(value_col).over(window) / days_in_window)


def _modes_per_group(df: DataFrame, group_col: str, value_cols: list[str]) -> DataFrame:
    """One row per ``group_col`` value, with each of ``value_cols`` replaced by
    its most frequent non-null (mode) value within that group.

    ``F.max()`` on a raw column would return the
    alphabetically/numerically largest value, not the most common one —
    e.g. a borough that appears once would beat one that appears 500 times
    if its name happens to sort later. This does an actual per-group,
    per-column frequency count and picks the top row via a window function,
    which is the standard "mode per group" pattern in Spark (there's no
    built-in mode aggregate).

    Null values are excluded from the count before ranking: on the real
    accident data, rows with a missing borough outnumber any single named
    borough in about half the months, so an unfiltered mode would mostly
    report "the most common value is missing" — technically the true mode,
    but useless as a "where do accidents happen" signal. A group whose rows
    are *all* null for a column still ends up with a null
    ``most_common_<column>`` (nothing to report), which is correct.
    """
    result = df.select(group_col).distinct()
    for value_col in value_cols:
        counts = df.filter(F.col(value_col).isNotNull()).groupBy(group_col, value_col).count()
        ranked = counts.withColumn(
            "_rank",
            F.row_number().over(Window.partitionBy(group_col).orderBy(F.desc("count"))),
        )
        mode_per_group = ranked.filter(F.col("_rank") == 1).select(
            group_col, F.col(value_col).alias(f"most_common_{value_col}")
        )
        result = result.join(mode_per_group, on=group_col, how="left")
    return result


# --- Claims ---------------------------------------------------------------


def build_daily_claims(silver_claims: DataFrame) -> DataFrame:
    daily = (
        silver_claims.withColumn("claim_date", F.to_date("claim_datetime"))
        .groupBy("claim_date")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum("injury_claim_amount").alias("injury_claim_amount"),
            F.sum("property_claim_amount").alias("property_claim_amount"),
            F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
            F.round(F.avg("incident_hour")).alias("average_incident_hour"),
            F.round(F.avg("driver_age")).alias("average_driver_age"),
        )
    )

    w = Window.orderBy("claim_date")
    return daily.withColumn("pct_change_number_of_claims", _pct_change("number_of_claims", w)).withColumn(
        "30d_rolling_avg_total_claim_amount", _rolling_daily_avg("total_claim_amount", "claim_date")
    )


def build_weekly_claims(silver_claims: DataFrame) -> DataFrame:
    weekly = (
        silver_claims.withColumn("claim_year_week", _year_week("claim_datetime"))
        .groupBy("claim_year_week")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum("injury_claim_amount").alias("injury_claim_amount"),
            F.sum("property_claim_amount").alias("property_claim_amount"),
            F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
            F.round(F.avg("incident_hour")).alias("average_incident_hour"),
            F.round(F.avg("driver_age")).alias("average_driver_age"),
        )
    )

    w = Window.orderBy("claim_year_week")
    return weekly.withColumn("pct_change_number_of_claims", _pct_change("number_of_claims", w)).withColumn(
        "4w_rolling_avg_total_claim_amount",
        F.round(F.avg("total_claim_amount").over(w.rowsBetween(-3, 0))),
    )


def build_monthly_claims(silver_claims: DataFrame) -> DataFrame:
    monthly = (
        silver_claims.withColumn("claim_year_month", _year_month("claim_datetime"))
        .groupBy("claim_year_month")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum("injury_claim_amount").alias("injury_claim_amount"),
            F.sum("property_claim_amount").alias("property_claim_amount"),
            F.sum("vehicle_claim_amount").alias("vehicle_claim_amount"),
            F.round(F.avg("incident_hour")).alias("average_incident_hour"),
            F.round(F.avg("driver_age")).alias("average_driver_age"),
        )
    )

    w = Window.orderBy("claim_year_month")
    return monthly.withColumn("pct_change_number_of_claims", _pct_change("number_of_claims", w)).withColumn(
        "3m_rolling_avg_total_claim_amount",
        F.round(F.avg("total_claim_amount").over(w.rowsBetween(-2, 0))),
    )


# --- Accidents --------------------------------------------------------------


def build_daily_accidents(silver_accidents: DataFrame) -> DataFrame:
    daily = silver_accidents.groupBy("accident_date").agg(
        F.count("*").alias("number_of_accidents"),
        F.round(F.avg("accident_hour")).alias("average_accident_hour"),
    )
    daily = daily.join(
        _modes_per_group(silver_accidents, "accident_date", ["borough", "zip_code"]),
        on="accident_date",
        how="left",
    )

    w = Window.orderBy("accident_date")
    return daily.withColumn(
        "pct_change_number_of_accidents", _pct_change("number_of_accidents", w)
    ).withColumn(
        "30d_rolling_avg_number_of_accidents", _rolling_daily_avg("number_of_accidents", "accident_date")
    )


def build_weekly_accidents(silver_accidents: DataFrame) -> DataFrame:
    accidents_with_week = silver_accidents.withColumn("accident_year_week", _year_week("accident_date"))

    weekly = accidents_with_week.groupBy("accident_year_week").agg(
        F.count("*").alias("number_of_accidents"),
        F.round(F.avg("accident_hour")).alias("average_accident_hour"),
    )
    weekly = weekly.join(
        _modes_per_group(accidents_with_week, "accident_year_week", ["borough", "zip_code"]),
        on="accident_year_week",
        how="left",
    )

    w = Window.orderBy("accident_year_week")
    return weekly.withColumn(
        "pct_change_number_of_accidents", _pct_change("number_of_accidents", w)
    ).withColumn(
        "4w_rolling_avg_number_of_accidents",
        F.round(F.avg("number_of_accidents").over(w.rowsBetween(-3, 0))),
    )


def build_monthly_accidents(silver_accidents: DataFrame) -> DataFrame:
    accidents_with_month = silver_accidents.withColumn("accident_year_month", _year_month("accident_date"))

    monthly = accidents_with_month.groupBy("accident_year_month").agg(
        F.count("*").alias("number_of_accidents"),
        F.round(F.avg("accident_hour")).alias("average_accident_hour"),
    )
    monthly = monthly.join(
        _modes_per_group(accidents_with_month, "accident_year_month", ["borough", "zip_code"]),
        on="accident_year_month",
        how="left",
    )

    w = Window.orderBy("accident_year_month")
    return monthly.withColumn(
        "pct_change_number_of_accidents", _pct_change("number_of_accidents", w)
    ).withColumn(
        "3m_rolling_avg_number_of_accidents",
        F.round(F.avg("number_of_accidents").over(w.rowsBetween(-2, 0))),
    )


# NYC's five boroughs fit inside this box. On the sample data it drops 1,264
# of 1.4M accidents: 1,226 geocoded to (0, 0) and a handful far outside NYC.
NYC_LATITUDE = (40.49, 40.92)
NYC_LONGITUDE = (-74.27, -73.68)
WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def build_accident_grid(silver_accidents: DataFrame, decimals: int = 3) -> DataFrame:
    """Accident counts per lat/lon grid cell (coordinates rounded to ``decimals``,
    ~110 m at 3), NYC area only — a map-ready density table instead of 1.4M raw points."""
    in_nyc = silver_accidents.filter(
        F.col("latitude").between(*NYC_LATITUDE) & F.col("longitude").between(*NYC_LONGITUDE)
    )
    return in_nyc.groupBy(
        F.round("latitude", decimals).alias("latitude"),
        F.round("longitude", decimals).alias("longitude"),
    ).agg(
        F.count("*").alias("number_of_accidents"),
        F.sum(F.coalesce(F.col("number_of_persons_injured"), F.lit(0))).alias("number_of_persons_injured"),
    )


def build_accidents_by_weekday_hour(silver_accidents: DataFrame) -> DataFrame:
    """Full 7 x 24 matrix of accident counts by ISO weekday (1 = Monday) and hour,
    with explicit zeros, so a heatmap never has holes. Rows without an hour are left out."""
    spark = silver_accidents.sparkSession
    # dayofweek() is 1 = Sunday .. 7 = Saturday; shift to ISO 1 = Monday .. 7 = Sunday.
    counts = (
        silver_accidents.filter(F.col("accident_hour").isNotNull())
        .withColumn("weekday_number", ((F.dayofweek("accident_date") + 5) % 7) + 1)
        .groupBy("weekday_number", "accident_hour")
        .agg(F.count("*").alias("number_of_accidents"))
    )
    # An inline VALUES table, so the 168 grid rows are built inside the JVM.
    # spark.createDataFrame() on a Python list would ship them through a
    # Python worker process instead, and starting that worker was the single
    # slowest thing in this table (~10s per run; see docs/benchmark.md).
    values = ", ".join(
        f"({i + 1}, '{name}', {hour})" for i, name in enumerate(WEEKDAY_NAMES) for hour in range(24)
    )
    grid = spark.sql(f"SELECT * FROM VALUES {values} AS grid(weekday_number, weekday, accident_hour)")
    return (
        grid.join(counts, on=["weekday_number", "accident_hour"], how="left")
        .fillna(0, ["number_of_accidents"])
        .select("weekday_number", "weekday", "accident_hour", "number_of_accidents")
    )


def build_monthly_accidents_by_borough(silver_accidents: DataFrame) -> DataFrame:
    """Monthly accidents per borough. Accidents with no borough keep a null
    ``borough`` row per month (22% of the sample, rising sharply after 2016),
    so dashboards can report them separately instead of losing them."""
    return (
        silver_accidents.withColumn("accident_year_month", _year_month("accident_date"))
        .groupBy("accident_year_month", "borough")
        .agg(F.count("*").alias("number_of_accidents"))
    )


# --- Policies ----------------------------------------------------------------


def build_monthly_policies(silver_policies: DataFrame) -> DataFrame:
    issued_policies = (
        silver_policies.withColumn("year_month", _year_month("issue_date"))
        .withColumn("issue_age_of_vehicle", F.year("issue_date") - F.col("model_year").cast("integer"))
        .groupBy("year_month")
        .agg(
            F.count("*").alias("policies_issued"),
            F.round(F.sum("sum_insured")).alias("exposure"),
            F.round(F.sum("premium")).alias("total_premium"),
            # One decimal: the monthly average only moves by fractions of a
            # year, so rounding to whole years turns the trend into steps.
            F.round(F.avg("issue_age_of_vehicle"), 1).alias("avg_issue_age_of_vehicle"),
        )
    )

    expired_policies = (
        silver_policies.withColumn("year_month", _year_month("expiry_date"))
        .groupBy("year_month")
        .agg(F.count("*").alias("policies_expired"))
    )

    # A left join would silently drop any month that only has expirations
    # and no new issuances (e.g. trailing months near the end of the sample
    # data, once issuances have tapered off but 1-year-old policies are
    # still expiring) — a full outer join keeps every month either side has.
    return (
        issued_policies.join(expired_policies, on="year_month", how="full")
        .fillna(0, ["policies_issued", "policies_expired", "total_premium"])
        .orderBy("year_month")
    )


# --- Loss ratio and vehicle segment risk -------------------------------------
#
# Every loss-ratio number here (monthly, overall, and per vehicle segment)
# uses the same basis: incurred claims that join to a (deduplicated) Silver
# policy, divided by the *written* premium of Silver policies. Written
# premium is the policy's full premium, counted in the month it was issued;
# it is not earned premium (spread over the policy term), which this
# pipeline doesn't compute. A claim whose policy didn't survive Silver
# (e.g. a negative premium) is left out of the numerator, since its premium
# is already missing from the denominator.
#
# The monthly ratio attributes each claim to the month of its *incident*
# (incident_date), not the month it was filed (claim_datetime) — the two
# differ for 64% of the sample claims. Premium goes to the month of
# issuance. Both sides are aggregated to month level separately before
# being joined, so a policy's premium is never counted once per claim.
#
# The sample data is synthetic and only about 29% of incidents fall inside
# their policy's effective-expiry period. Those claims are still counted
# (dropping 71% of claims would make the ratio meaningless too), but
# gold_loss_ratio_monthly carries a per-month count of them and run_gold
# logs the total, so the gap is visible rather than silent.


def _claims_on_silver_policies(
    silver_claims: DataFrame, silver_policies: DataFrame, policy_cols: list[str]
) -> DataFrame:
    """Claims joined (inner) to their Silver policy, with ``policy_cols`` from
    the policy. ``silver_policies`` is unique on ``policy_number``, so this
    never duplicates a claim."""
    return silver_claims.join(
        silver_policies.select("policy_number", *policy_cols), on="policy_number", how="inner"
    )


def _outside_policy_period(incident_col: str = "incident_date") -> F.Column:
    """1 if the incident is definitely outside the policy's effective-expiry
    period, else 0 (including when either policy date is missing)."""
    inside = F.col(incident_col).between(F.col("effective_date"), F.col("expiry_date"))
    return F.when(~inside, F.lit(1)).otherwise(F.lit(0))


def _loss_ratio_pct(claims_col: str, premium_col: str) -> F.Column:
    """Claims / premium as a percentage; null (not 0, not an error) when there's no premium."""
    return F.when(
        F.col(premium_col) > 0,
        F.round((F.col(claims_col) / F.col(premium_col)) * 100, 2),
    )


def build_monthly_loss_ratio(silver_claims: DataFrame, silver_policies: DataFrame) -> DataFrame:
    claims_by_month = (
        _claims_on_silver_policies(silver_claims, silver_policies, ["effective_date", "expiry_date"])
        .withColumn("year_month", _year_month("incident_date"))
        .groupBy("year_month")
        .agg(
            F.count("*").alias("number_of_claims"),
            F.sum("total_claim_amount").alias("total_claim_amount"),
            F.sum(_outside_policy_period()).alias("claims_outside_policy_period"),
        )
    )
    premium_by_month = (
        silver_policies.withColumn("year_month", _year_month("issue_date"))
        .groupBy("year_month")
        .agg(F.round(F.sum("premium")).alias("total_premium"))
    )

    # Full join: incident months before the first policy issue (the sample
    # has incidents back to 2010, premium from 2015) have claims but no
    # premium, and still need to show up — with a null ratio, not 0.
    loss_ratio = (
        claims_by_month.join(premium_by_month, on="year_month", how="full")
        .fillna(
            0, ["number_of_claims", "total_claim_amount", "claims_outside_policy_period", "total_premium"]
        )
        .withColumn("loss_ratio_pct", _loss_ratio_pct("total_claim_amount", "total_premium"))
    )

    # Ratio of the 3-month sums rather than an average of three monthly
    # ratios, so a month with a tiny premium can't dominate the average.
    # Null wherever the month itself has no premium, so the line stops
    # instead of carrying a stale value forward.
    w = Window.orderBy("year_month").rowsBetween(-2, 0)
    rolling_claims = F.sum("total_claim_amount").over(w)
    rolling_premium = F.sum("total_premium").over(w)
    return loss_ratio.withColumn(
        "3m_rolling_avg_loss_ratio_pct",
        F.when(F.col("total_premium") > 0, F.round((rolling_claims / rolling_premium) * 100, 2)),
    ).orderBy("year_month")


def _segment_risk(silver_claims: DataFrame, silver_policies: DataFrame, group_col: str) -> DataFrame:
    """One row per distinct, non-blank value of ``group_col`` (a column on
    ``silver_policies``, e.g. vehicle body type or usage), with policy count,
    premium, exposure, claim count/severity, claim frequency, and loss ratio
    for that segment. Same basis as :func:`build_monthly_loss_ratio`: claims
    that join to a Silver policy over that segment's written premium.

    Blank-string values (present in the raw feed but not caught by Silver's
    null-only filtering) are excluded rather than lumped into an "Unknown"
    bucket, since they're a small share of rows and would otherwise dominate
    a chart sorted by claim count without being an actual segment.
    """
    policies = silver_policies.filter(F.col(group_col).isNotNull() & (F.col(group_col) != ""))

    policy_stats = policies.groupBy(group_col).agg(
        F.count("*").alias("number_of_policies"),
        F.round(F.sum("premium")).alias("total_premium"),
        F.round(F.sum("sum_insured")).alias("exposure"),
    )

    claim_stats = (
        _claims_on_silver_policies(silver_claims, policies, [group_col])
        .groupBy(group_col)
        .agg(
            F.count("*").alias("number_of_claims"),
            F.round(F.sum("total_claim_amount")).alias("total_claim_amount"),
            F.round(F.avg("total_claim_amount")).alias("avg_claim_severity"),
        )
    )

    combined = policy_stats.join(claim_stats, on=group_col, how="left").fillna(
        0, ["number_of_claims", "total_claim_amount"]
    )

    return combined.withColumn(
        "claims_per_100_policies",
        F.round((F.col("number_of_claims") / F.col("number_of_policies")) * 100, 2),
    ).withColumn("loss_ratio_pct", _loss_ratio_pct("total_claim_amount", "total_premium"))


def build_vehicle_body_risk(silver_claims: DataFrame, silver_policies: DataFrame) -> DataFrame:
    return (
        _segment_risk(silver_claims, silver_policies, "body")
        .withColumnRenamed("body", "vehicle_body")
        .orderBy(F.desc("total_premium"))
    )


def build_vehicle_usage_risk(silver_claims: DataFrame, silver_policies: DataFrame) -> DataFrame:
    return _segment_risk(silver_claims, silver_policies, "vehicle_usage").orderBy(F.desc("total_premium"))


OTHER_BODY = "Other (< 20 policies)"
UNKNOWN_BODY = "Unknown body"


def build_loss_ratio_contribution(
    silver_claims: DataFrame, silver_policies: DataFrame, min_policies: int = 20
) -> DataFrame:
    """How far each vehicle body pushes the portfolio loss ratio above or below 100%.

    ``contribution_pct_points`` = (body's claims - body's premium) / *portfolio*
    premium * 100, so the contributions of all rows add up exactly to the
    portfolio loss ratio minus 100 — the waterfall's total. Same basis as
    every other loss ratio here (claims on Silver policies / written
    premium). Unlike ``gold_vehicle_body_risk``, nothing is excluded: blank
    bodies become ``Unknown body`` and bodies under ``min_policies`` are
    pooled into one ``Other`` row, so the total still reconciles.
    """
    body = F.when(F.trim(F.coalesce(F.col("body"), F.lit(""))) == "", F.lit(UNKNOWN_BODY)).otherwise(
        F.col("body")
    )
    policies = silver_policies.withColumn("raw_body", body)
    sizes = policies.groupBy("raw_body").agg(F.count("*").alias("_n"))
    policies = (
        policies.join(sizes, on="raw_body")
        .withColumn(
            "vehicle_body",
            F.when(
                (F.col("_n") < min_policies) & (F.col("raw_body") != UNKNOWN_BODY), F.lit(OTHER_BODY)
            ).otherwise(F.col("raw_body")),
        )
        .select("policy_number", "vehicle_body", "premium")
    )

    premium = policies.groupBy("vehicle_body").agg(
        F.count("*").alias("number_of_policies"), F.sum("premium").alias("total_premium")
    )
    claims = (
        _claims_on_silver_policies(silver_claims, policies, ["vehicle_body"])
        .groupBy("vehicle_body")
        .agg(F.sum("total_claim_amount").alias("total_claim_amount"))
    )
    combined = premium.join(claims, on="vehicle_body", how="left").fillna(0, ["total_claim_amount"])
    portfolio_premium = F.sum("total_premium").over(Window.partitionBy())
    return combined.withColumn(
        "contribution_pct_points",
        F.round((F.col("total_claim_amount") - F.col("total_premium")) / portfolio_premium * 100, 4),
    ).withColumn("loss_ratio_pct", _loss_ratio_pct("total_claim_amount", "total_premium"))


def _write(df: DataFrame, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table_name)


def run_gold(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Read all Silver tables, aggregate, and write ``gold_*`` Delta tables."""

    silver_claims = spark.table("silver_claims")
    _write(build_daily_claims(silver_claims), "gold_claims_daily")
    _write(build_weekly_claims(silver_claims), "gold_claims_weekly")
    _write(build_monthly_claims(silver_claims), "gold_claims_monthly")

    silver_accidents = spark.table("silver_accidents")
    _write(build_daily_accidents(silver_accidents), "gold_accidents_daily")
    _write(build_weekly_accidents(silver_accidents), "gold_accidents_weekly")
    _write(build_monthly_accidents(silver_accidents), "gold_accidents_monthly")
    _write(build_accident_grid(silver_accidents), "gold_accident_grid")
    _write(build_accidents_by_weekday_hour(silver_accidents), "gold_accidents_weekday_hour")
    _write(build_monthly_accidents_by_borough(silver_accidents), "gold_accidents_borough_monthly")

    silver_policies = spark.table("silver_policies")
    monthly_policies = build_monthly_policies(silver_policies)
    _write(monthly_policies, "gold_policies_monthly")

    loss_ratio_monthly = build_monthly_loss_ratio(silver_claims, silver_policies)
    _write(loss_ratio_monthly, "gold_loss_ratio_monthly")
    totals = (
        spark.table("gold_loss_ratio_monthly")
        .agg(F.sum("number_of_claims"), F.sum("claims_outside_policy_period"))
        .first()
    )
    logger.warning(
        "%s of %s claims on Silver policies have an incident date outside the policy's "
        "effective-expiry period; they are still counted in the loss ratio.",
        totals[1],
        totals[0],
    )
    _write(build_vehicle_body_risk(silver_claims, silver_policies), "gold_vehicle_body_risk")
    _write(build_vehicle_usage_risk(silver_claims, silver_policies), "gold_vehicle_usage_risk")
    _write(build_loss_ratio_contribution(silver_claims, silver_policies), "gold_loss_ratio_contribution")
