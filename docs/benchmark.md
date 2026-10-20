# Spark scale benchmark

The sample data is small (1.6M raw accidents, 227k policies, 5.6k claims), so a normal run says little about how the pipeline behaves as data grows. This benchmark scales the three sources up to 20x and times each step. It then uses Spark's event logs and query plans to find where the time actually goes, and applies only the optimizations those measurements justify, one at a time.

**Headline (local mode, 16 threads, same machine, before/after runs back to back):**

| scale | before | after | change |
|---|---:|---:|---:|
| x1 (sample) | 177.7 s | 153.4 s | −14% |
| x5 | 514.3 s | 405.4 s | −21% |
| x10 | 723.0 s | 564.5 s | −22% |
| x20 | **did not finish**: out of memory in the quality step after ~1,580 s | 837.4 s | now completes |

All 14 Gold tables produced from the real sample data are **identical** before and after: same schema, same rows, same values, with no float tolerance needed. See [Gold outputs are unchanged](#gold-outputs-are-unchanged).

The optimized code also completes every scale, x20 included, on the Docker Compose Spark cluster (2 workers × 2 cores): 134 s, 303 s, 417 s and 826 s. See [Cluster mode](#cluster-mode).

- [How the scaled data is generated](#how-the-scaled-data-is-generated)
- [Setup](#setup)
- [Results](#results)
- [Cluster mode](#cluster-mode)
- [Where the time went (baseline)](#where-the-time-went-baseline)
- [Optimizations that were kept](#optimizations-that-were-kept)
- [Tried and not kept](#tried-and-not-kept)
- [Checked and not a bottleneck](#checked-and-not-a-bottleneck)
- [Gold outputs are unchanged](#gold-outputs-are-unchanged)
- [Caveats](#caveats)
- [Reproducing it](#reproducing-it)

## How the scaled data is generated

`scripts/scale_data.py --scale N` writes `data/benchmark/x<N>/`, with the same layout as `data/samples/` (`mysql/policies.csv`, `mongodb/claims.json`, `s3/external/accidents.csv.gz`). Pointing `DATA_ROOT` at it is all the pipeline needs. `data/samples/` is only ever read, and `data/benchmark/` is git-ignored.

Each source becomes N back-to-back copies of the original. Copy 0 is the original records unchanged. Every later copy *k* changes **only its keys**:

| key | rule | why |
|---|---|---|
| `POLICY_NO` (policies) | `"<original>-x<k>"` (empty stays empty) | It's a string in the source (values like `101610176/1`), so a suffix keeps Spark's inferred type. |
| `claim_no`, `policy_no` (claims) | `"<original>-x<k>"` | Copy *k*'s claim points at copy *k*'s policy, so the claims-to-policies join behaves as on the original. |
| `COLLISION_ID` (accidents) | `id + k × 10,000,000` | The largest sample id is ~4.2M, so copies never collide, and x20 stays below 2³¹ (still an `int`). |

Everything else is byte-for-byte the same:
- The policies CSV is processed line by line with quote tracking, so the 22 records with a newline inside a quoted field survive.
- In accidents, a quote-aware split handles the 3 rows with a comma inside a quoted field.
- The accidents output is again a single `.gz` file, so it stays unsplittable, exactly like the original.

As a result, Silver's dedup (7 duplicate policies per copy), the quality gate's uniqueness checks and the loss-ratio join all work on copy *k* exactly as on the original. **Additive Gold totals come out at exactly N times the sample's, while ratios and Gold row counts don't change.** `scripts/benchmark.py` checks this on every run: claims, accidents and policies issued are exactly N × the x1 numbers, and the overall loss ratio stays 84.97%. It held on every run.

| scale | policies | claims | accidents | on disk |
|---|---:|---:|---:|---:|
| x1 | 227,484 | 5,627 | 1.6M | 97 MB |
| x5 | 1.1M | 28,135 | 8.1M | 477 MB |
| x10 | 2.3M | 56,270 | 16.1M | 955 MB |
| x20 | 4.5M | 112,540 | 32.2M | 1.9 GB |

## Setup

- **Machine:** Windows 11 laptop, Intel i7-11800H (8 cores / 16 threads), 16 GB RAM, of which only ~2–4 GB was free during the runs (other apps open), JDK 17, Python 3.11.
- **Spark:** PySpark 3.5.3 + Delta 3.2.0, `local[*]` (16 task slots), driver heap 2 GB. This is the pipeline's own `get_local_spark_session`, unchanged.
- **Runner:** `scripts/benchmark.py` runs bronze → silver → quality → gold (not the ClickHouse load) with `SOURCE_MODE=files`, `DATA_ROOT` pointing at the scaled folder and a fresh `WAREHOUSE_DIR` under `benchmark-runs/` for every run, so the real `./warehouse` is never touched.
- **One process per run, cache cleared between steps:** in production each step is a separate Airflow task, i.e. a separate process. Clearing Spark's cache between steps keeps a single-process run honest about that (see [the Great Expectations cache](#3-great-expectations-validates-without-caching-the-table)).
- **Event logging:** each run's Spark event log (`benchmark-runs/events/`) is enabled through a generated `spark-defaults.conf`, so the pipeline code doesn't need to know about it.
- **Job labels:** the runner labels every Spark job `"<step> | <action> <table>"`, so `scripts/analyze_event_log.py` can attribute every stage, including schema-inference passes, to a step and a table.
- **Cluster mode** ran separately, later the same day, once enough RAM was free (see [Cluster mode](#cluster-mode)). The cluster needs ~6.5 GB on its own (2 workers × 1.5 GB, a 2 GB driver, the master). That wasn't available while the local runs were going on, with only 2–4 GB free. Everything outside that section is local mode.

## Results

Seconds per step, local mode. Each **before**/**after** pair ran back to back, so both runs see the same machine conditions; see [Caveats](#caveats) for why that matters here. "before" is the code at commit `c1841fa` (run via `--src`); "after" is this change.

| scale | code | bronze | silver | quality | gold | total |
|---|---|---:|---:|---:|---:|---:|
| x1 | before | 40.5 | 70.1 | 20.7 | 46.4 | 177.7 |
| x1 | after | 43.1 | 66.4 | 12.1 | 31.9 | **153.4** |
| x5 | before | 168.9 | 234.7 | 55.9 | 54.8 | 514.3 |
| x5 | after | 121.5 | 226.6 | 12.4 | 45.0 | **405.4** |
| x10 | before | 309.9 | 264.4 | 77.6 | 71.1 | 723.0 |
| x10 | after | 232.3 | 269.4 | 13.3 | 49.4 | **564.5** |
| x20 | before | 592.7 | 297.4 | **OOM after ~690 s** | not reached | **failed** |
| x20 | after | 456.2 | 302.3 | 15.6 | 63.2 | **837.4** |

Per step at x10: Bronze −25%, quality −83%, Gold −31%, Silver unchanged. At x20 the original code doesn't complete at all. Its quality step runs the 2 GB driver out of heap ([log excerpt](benchmark/before-x20-quality-oom.txt)). The automatic retry was stopped, since the failure is deterministic; it was clear from the first attempt. Why Silver doesn't move is explained in [Tried and not kept](#accidents-repartition--the-biggest-win-not-applied).

Raw results for every run, including the one-change-at-a-time experiments, are in [`benchmark/results.csv`](benchmark/results.csv) and [`benchmark/results.json`](benchmark/results.json). The event-log summaries cited below are in [`benchmark/`](benchmark/).

## Cluster mode

The same optimized code (`--label after`), run with `--mode cluster` on Docker Compose's `cluster` profile. The cluster had one master and two workers, each worker giving 2 cores and a 1 GB executor, for **4 task slots** in total. The driver ran in the `benchmark-driver` container with a 2 GB heap. Docker's VM had 7.8 GB, of which 6.7 GB was available at the start. No other Compose services were running. The `before` code was not run on the cluster.

| scale | bronze | silver | quality | gold | total | local (16 slots) | change |
|---|---:|---:|---:|---:|---:|---:|---:|
| x1 | 40.6 | 51.1 | 15.4 | 27.1 | **134.1** | 153.4 | −13% |
| x5 | 99.1 | 151.9 | 15.3 | 36.4 | **302.7** | 405.4 | −25% |
| x10 | 173.2 | 181.0 | 16.3 | 46.1 | **416.6** | 564.5 | −26% |
| x20 | 362.5 | 379.6 | 19.4 | 64.3 | **825.8** | 837.4 | −1% |

**x20 completed on the first attempt, with no tuning.** It hit no out-of-memory error, no task was retried, and each worker container used about 2 GB in `docker stats` snapshots taken during the run. The scaling checks held at every scale: claims, accidents and policies issued were exactly N× the x1 numbers, the loss ratio stayed 84.97%, and Gold row counts matched the local runs.

**Why 4 slots match or beat 16:** the run time is set by the stages that are one task long, not by how many slots there are. From the x10 and x20 event logs:

| stage | local x10 | cluster x10 | local x20 | cluster x20 |
|---|---:|---:|---:|---:|
| bronze: write `bronze_accidents` (1 task, gzip) | 193.8 s | 138.5 s | 397.5 s | 305.5 s |
| silver: write `silver_accidents` | 243.4 s | 160.0 s | 275.0 s (1 task did all of it) | 340.2 s (6 tasks, 2 waves) |

- **The gzip read and the Bronze accidents write are a single task in both modes**, so extra slots can't help them. That one task ran ~25–30% faster on the cluster. The cluster is a Linux VM reading from a Docker volume; local mode is Windows reading from NTFS while the laptop was paging. The two sets of runs weren't interleaved, so this is a difference in conditions, not something the cluster itself buys.
- **At x20, the Silver accidents write is where the cluster loses.** Local mode ran it as one 275 s task. The cluster split it into 6 tasks, but 6 tasks on 4 slots means two waves: four tasks at ~201 s, then the last two at 15 s and 139 s. That makes 340 s, enough to cancel out the cluster's Bronze advantage at x20.
- **Startup took longer:** 10–11 s versus ~5 s locally, and 34 s at x20, while the executors registered with the master. Startup isn't part of the step times above.

The `parallelism` column for cluster runs in `results.csv` reads **2**, not 4. The runner records `defaultParallelism` right after the session starts, before the executors have registered, and Spark reports its minimum of 2 until then. The event logs confirm two executors with 2 cores each.

What this shows: the pipeline runs correctly on a real multi-process cluster with executors on other hosts, at every scale, without changing any code. It also shows that **adding slots won't speed it up much until the single-task accidents path is fixed**; see [Tried and not kept](#accidents-repartition--the-biggest-win-not-applied).

## Where the time went (baseline)

From the x10 baseline event log ([summary](benchmark/eventlog-baseline-local-x10.md)). "Longest task" is the slowest single task in the stage. When it equals the stage's duration, one task did all the work while the other 15 slots sat idle.

| stage | duration | tasks | longest task |
|---|---:|---:|---:|
| silver: write `silver_accidents` | 163.6 s | 16 | **163.6 s** |
| bronze: write `bronze_accidents` | 137.5 s | **1** | 137.5 s |
| bronze: *read* `accidents.csv.gz` (schema inference) | 45.9 s | **1** | 45.9 s |
| quality: validate `silver_accidents` | 39.3 s | 16 | **39.3 s** |
| gold: write `gold_accidents_weekday_hour` | 10.4 s | 16 | 10.4 s (no input data at all) |

Four of the five slowest stages trace back to the same root cause:

1. **`accidents.csv.gz` is gzip**, which has no block boundaries, so Spark has to read it in **one task**. (The policies CSV is the same, because `multiLine` makes it unsplittable too.)
2. Bronze therefore wrote `bronze_accidents` in **one task**, as one Parquet file with effectively one row group.
3. Silver split that file into 16 tasks, but a row group can't be split, so **one task got all 16M rows** and 15 got none.
4. That one task wrote one Silver file, so the quality checks again ran on **one task**.

On top of that, Bronze **read the gzip file twice**: once to infer the schema, once for real.

## Optimizations that were kept

Each was applied on its own and measured against the code before it, in interleaved pairs: current, candidate, current, candidate. Each was also checked for identical Gold output on the sample data.

### 1. Explicit Bronze schemas instead of `inferSchema`

**Change:** `src/insurance_pipeline/source_schemas.py` holds the three file schemas as DDL strings. `ingest_raw_data` reads with `.schema(...)` instead of `inferSchema=True` (and JSON inference for claims).

**Measured (x5, 2 runs each):** Bronze 116.1 s → 86.8 s (**−25%**), total 358.7 s → 326.9 s.

**Why it helped:** with `inferSchema`, Spark has to know every column's type before it can plan the read. So it first scans the *entire* file just to guess the types, then reads it again for real. For a gzipped file that's a full single-threaded decompress-and-parse that produces nothing. An explicit schema skips the guessing pass, so the expensive single-task read happens once instead of twice. (The read is still a single task; that's inherent to gzip.)

**Why it's safe:** the schemas are exactly what Spark inferred from the sample files, odd types included. `ACCIDENT_DATE` and `ZIP_CODE` stay strings, for instance, because a few raw rows don't parse, so Bronze's output is unchanged. `test_explicit_bronze_schemas_equal_what_spark_infers` reads each sample file both ways and asserts the schemas are equal. If the source data changes shape, CI fails, rather than an explicit schema silently nulling out values it can't parse. `databases` mode (MySQL/Mongo) is unchanged.

### 2. Repartition the policies before Bronze writes them

**Change:** `spread_across_cores()` in `bronze.py` round-robins the policies DataFrame into one partition per core before `saveAsTable`.

**Measured:** a modest gain, so it's judged on stage times; the run-to-run noise in total time was larger than the effect. At x5, `silver_policies` went 9.0 s → 7.6 s even though that run was on a slower machine phase: unchanged accident stages in the same run were ~55% slower. The policies write now uses 16 tasks for encoding instead of 1. Gold is identical.

**Why it helped:** the policies CSV needs `multiLine` (22 records have a newline inside a quoted field), and that makes it unsplittable, so it arrives as one partition. Without a repartition, one task writes one Parquet file, and Silver's transform and dedup inherit that single partition. `repartition(n)` adds one shuffle, about 150 MB at x5, but after it the Parquet encoding and everything downstream run on all cores.

### 3. Great Expectations validates without caching the table

**Change:** `context.data_sources.add_spark(..., persist=False)` in `quality.py`.

**Measured (x5, interleaved, 2 pairs):** quality step 56.8 / 58.8 s → 13.8 / 13.7 s (**−76%**). In the final runs: x10 77.6 s → 13.3 s (**−83%**). At **x20 it's the difference between failing and passing**:
- **Before:** the cache doesn't fit, and Spark logs `MemoryStore: Not enough space to cache rdd_342_…` and then `java.lang.OutOfMemoryError: Java heap space`. Great Expectations catches the failed metric jobs and reports them as *failed expectations*, so the original pipeline rejected perfectly valid data ("`collision_id` has nulls" on a table Silver had just filtered to non-null) after ~11 minutes.
- **After:** the same check passes in 15.6 s.

**Why it helped:** Great Expectations' Spark engine calls `.persist()` on every DataFrame it validates, by default and with no unpersist. That copies *every column* of the Silver table into Spark's in-memory columnar cache before computing anything. For `silver_accidents` at x5 that meant decoding all ~30 columns of 7M rows in a single task (46.8 s) and holding them in a 2 GB heap. The expectations themselves only look at 3–4 columns. Without the cache, each metric query reads just those columns straight from Parquet, and Parquet is columnar, so the other columns are never decoded: 76 MB of input instead of 4.2 GB of scans, 3.3 s instead of 46.8 s. The pass/fail result is the same. `test_validate_dataframe_does_not_cache_the_table` guards it.

**How it was found:** the first benchmark runs executed all four steps in one SparkSession. The Gold plans then showed `InMemoryTableScan` over `silver_accidents` even though Gold never caches anything. Great Expectations had left the table cached, and Gold was quietly reusing it. In production each step is its own Airflow task, its own process, so Gold never gets that cache, but quality still pays to build it. The runner now clears the cache between steps to match production, and the quality step stopped building a cache nobody uses.

### 4. Build Gold's weekday × hour grid inside the JVM

**Change:** `build_accidents_by_weekday_hour` creates its fixed 7 × 24 grid with an inline SQL `VALUES` table instead of `spark.createDataFrame(<python list>)`.

**Measured (x1, interleaved):** that table 10.7 s → 1.3 s; Gold 53.8 s → 33.9 / 37.9 s. The saving is a fixed ~10 s per run at every scale.

**Why it helped:** `createDataFrame` on a Python list pickles the rows into an RDD that only a **Python worker process** can decode. In this pipeline, which is otherwise pure DataFrame/SQL, that was the only thing that needed a Python worker. Spark had to spawn one, and on Windows that takes ~10 s for 168 rows. A `VALUES` table is built entirely in the JVM, so no Python worker ever starts. On a cluster the same cost would hit every executor.

## Tried and not kept

### Accidents repartition: the biggest win, not applied

Applying the same `spread_across_cores()` to accidents before the Bronze write was by far the most effective change. At x5, Silver went 155 s → 57–65 s, quality 39 s → 24–28 s, and the total 327 s → 225–272 s ([event-log summary](benchmark/eventlog-repartition-accidents-local-x5.md)). It fixes the root cause above: Bronze writes 16 files, so Silver and quality get 16 real tasks instead of 1.

**It changed Gold**, so it isn't applied. `compare_gold.py` found 35 values that differ on the sample data: `most_common_zip_code` in 34 rows of `gold_accidents_daily` and 1 row of `gold_accidents_weekly`. Every other value in every other table was identical. The cause is a pre-existing problem in Gold, not in the optimization. `_modes_per_group` ranks values per group with `row_number().over(Window.partitionBy(group).orderBy(desc("count")))`. When two zip codes tie for most accidents on a day (728 days in the sample have a tie), the winner is whichever row physically arrives first. That depends on how the rows are spread across partitions and shuffles. I checked whether the baseline's choices followed any simple rule ("first seen in the file", "lowest value", "lowest hash partition"); the best matched only 47–55% of tied days, no better than a coin flip. The baseline output is stable today only because accidents happen to flow through a single task.

Recommended follow-up (one line each):
1. Make the tie-break deterministic: `orderBy(F.desc("count"), F.asc(value_col))`.
2. Then apply `spread_across_cores()` to accidents too.

That changes those 35 cells once, from an arbitrary choice to a defined one. `most_common_zip_code`/`most_common_borough` aren't used by any dashboard query in `metabase_queries/`. It was left out here only because this change had to keep Gold identical. The repartition also costs memory: shuffling 1 GB through a 2 GB heap spilled 3.7 GB to disk at x5, which a bigger heap would avoid.

## Checked and not a bottleneck

Verified in the physical plans (`analyze_event_log.py --plans`, using the final adaptive plan) rather than assumed:

| suspect | what the plans/event log show | verdict |
|---|---|---|
| Shuffle joins between claims and policies | Every claims↔policies join is a `BroadcastHashJoin`: AQE broadcasts the small side, so there's no shuffle of the policy table. | Already optimal; no change. |
| Default 200 shuffle partitions | Every `Exchange hashpartitioning(…, 200)` is followed by `AQEShuffleRead coalesced`: adaptive execution already merges the 200 tiny partitions. | No tuning needed. |
| Unpartitioned global windows in Gold (`Window.orderBy(...)` with no `partitionBy`) | Spark warns "moving all data to a single partition", and the plans do show `Exchange SinglePartition`. But these run *after* aggregation, on ≤ 2,616 rows (one per day) and a few KB. | Harmless at any scale here; no change. |
| Caching `silver_accidents` in Gold (it's scanned by 6 tables) | After the changes above, no Gold table takes more than ~5 s at x5; the whole Gold step is ~40 s across 14 tables. | Not worth holding 32M rows in a 2 GB heap; not attempted. |

## Gold outputs are unchanged

`scripts/compare_gold.py` makes the before/after check explicit:
- `snapshot` reads every `gold_*` Delta table, sorts it by all columns and writes it as canonical JSON lines, plus each column's type and nullability.
- `compare` then checks two snapshots table by table: same tables, schema and row count, and every value equal. Floats are allowed a 1e-9 relative tolerance for summation order, though none was needed.

Result for the final code vs the original code (commit `c1841fa`), both run on `data/samples` in `files` mode:

```
gold_accident_grid: identical (48271 rows, exact)
gold_accidents_borough_monthly: identical (534 rows, exact)
gold_accidents_daily: identical (2616 rows, exact)
gold_accidents_monthly: identical (89 rows, exact)
gold_accidents_weekday_hour: identical (168 rows, exact)
gold_accidents_weekly: identical (386 rows, exact)
gold_claims_daily: identical (1381 rows, exact)
gold_claims_monthly: identical (72 rows, exact)
gold_claims_weekly: identical (314 rows, exact)
gold_loss_ratio_contribution: identical (43 rows, exact)
gold_loss_ratio_monthly: identical (91 rows, exact)
gold_policies_monthly: identical (88 rows, exact)
gold_vehicle_body_risk: identical (90 rows, exact)
gold_vehicle_usage_risk: identical (12 rows, exact)
ALL GOLD TABLES IDENTICAL
```

The overall loss ratio is 84.97% and `gold_policies_monthly` has 88 rows, matching the invariants in the project notes. Each kept optimization was also compared on its own as it was added.

## Caveats

- **A busy laptop, not a quiet server.** Free memory was 2–4 GB, Windows was paging, and other apps were in use. The same code took 359 s at x5 in the morning and 514 s in the afternoon. That's why every comparison above is an interleaved pair, why the small policies change is judged on its own stage times, and why absolute numbers matter less than the before/after ratio.
- **Single runs at x10/x20.** The final matrix runs one before/after pair per scale; the per-optimization experiments used 2 pairs at x5 (or x1 for the fixed-cost grid change).
- **Gold *cardinality* doesn't grow with scale.** The copies share dates, so Gold has the same number of rows at x20 as at x1; only the volume aggregated into them grows. That's deliberate: it keeps every Gold number checkable, at exactly N× or unchanged.
- **The remaining bottleneck is the single-task accidents path.** At x10 after the changes, Silver accidents is still one 16M-row task. The fix is known (see above) but held back to keep Gold identical.

## Reproducing it

```bash
pip install -e ".[dev]"
python scripts/scale_data.py --scale 5 10 20                     # -> data/benchmark/x5, x10, x20

# Original code, for comparison
mkdir -p benchmark-runs/baseline-src && git archive c1841fa src | tar -x -C benchmark-runs/baseline-src

python scripts/benchmark.py run --scales 1 5 10 20 --label before --src benchmark-runs/baseline-src/src
python scripts/benchmark.py run --scales 1 5 10 20 --label after
python scripts/benchmark.py report                                 # median tables
python scripts/analyze_event_log.py benchmark-runs/events/<run>/ --plans

# Gold equality on the sample data
python scripts/benchmark.py run --scales 1 --label before --src benchmark-runs/baseline-src/src --keep-warehouse
python scripts/benchmark.py run --scales 1 --label after --keep-warehouse
python scripts/compare_gold.py snapshot --warehouse benchmark-runs/warehouse/<before run>/warehouse --out benchmark-runs/gold-snapshots/before
python scripts/compare_gold.py snapshot --warehouse benchmark-runs/warehouse/<after run>/warehouse --out benchmark-runs/gold-snapshots/after
python scripts/compare_gold.py compare benchmark-runs/gold-snapshots/before benchmark-runs/gold-snapshots/after
```

**Cluster mode** (Docker Compose `cluster` profile: master + 2 workers × 2 cores / 1.5 GB) needs ~6.5 GB free for Docker:

```bash
docker compose build                       # the insurance-pipeline image
python scripts/benchmark.py run --mode cluster --scales 1 5 10 20 --label after
```

On a slow or flaky connection (e.g. through a VPN), the image build's `apt-get update` can fail with `Connection failed` on `deb.debian.org`; just rerun the build. The runner starts only `spark-master` and both workers, using the overlay `docker/benchmark/docker-compose.benchmark.yml`. The overlay leaves `docker-compose.yml` unchanged and adds a `benchmark-driver` container plus two volumes, `benchmark-data` and `benchmark-warehouse`. The runner copies the scaled data into `benchmark-data`, so reads don't go through Docker Desktop's Windows file sharing. It mounts the chosen `--src` over the image's editable install, so before/after need no image rebuild.

Scratch output (warehouses, event logs, Gold snapshots) goes to `benchmark-runs/`, which is git-ignored like `data/benchmark/`.
