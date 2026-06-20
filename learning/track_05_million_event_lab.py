#!/usr/bin/env python3
"""Track 05 — Million-event lab: all 4 curriculum lessons at scale (RAM-aware profiles).

  Lab A (Track 03) — short streaming demo (checkpoint/watermark) + batch fill to target row count
  Lab B (Track 01) — small files, partitionBy, coalesce
  Lab C (Track 02) — explain(), broadcast join, window ranking
  Lab D (Track 04) — skew war-room: naive vs salted join + JSON summary

Profiles (TRACK05_PROFILE):
  lite     — default; ~100k rows total, local[1]/1g; fits Docker on 8–16 GB RAM laptops
  standard — ~500k rows; needs ~16 GB+ RAM
  million  — ~1M rows; needs 32 GB+ RAM / generous Docker memory

Run:
  ./learning/run_track05_million.sh
  TRACK05_PROFILE=lite ./learning/run_track05_million.sh   # explicit (default)
  TRACK05_PROFILE=million TRACK_DRIVER_MEMORY=6g ...       # only if you have RAM
"""
import json
import os
import shutil
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from _common import banner, run_track, section

# ---------------------------------------------------------------------------
# Profiles — applied via setdefault so explicit env vars always win.
# See learning/00_START_HERE.md § "Local RAM limits" for why lite is default
# on laptops (Py4JNetworkError / batch falling behind = driver OOM signals).
# ---------------------------------------------------------------------------
PROFILE_PRESETS = {
    "lite": {
        "TRACK05_EVENTS": "100000",
        "TRACK05_STREAM_ROWS": "15000",
        "TRACK05_ROWS_PER_SEC": "1500",
        "TRACK05_MAX_WAIT_SEC": "45",
        "TRACK05_TRIGGER_SEC": "3",
        "TRACK05_ANALYTICS_ROWS": "50000",
        "TRACK05_BATCH_CHUNK": "25000",
        "TRACK05_SOURCE_PARTS": "2",
        "TRACK05_WRITE_BUCKETS": "50",
        "TRACK_SPARK_MASTER": "local[1]",
        "TRACK_DRIVER_MEMORY": "1g",
        "TRACK_EXECUTOR_MEMORY": "512m",
        "TRACK05_SHUFFLE_PARTITIONS": "8",
    },
    "standard": {
        "TRACK05_EVENTS": "500000",
        "TRACK05_STREAM_ROWS": "50000",
        "TRACK05_ROWS_PER_SEC": "3000",
        "TRACK05_MAX_WAIT_SEC": "120",
        "TRACK05_TRIGGER_SEC": "3",
        "TRACK05_ANALYTICS_ROWS": "100000",
        "TRACK05_BATCH_CHUNK": "50000",
        "TRACK05_SOURCE_PARTS": "4",
        "TRACK05_WRITE_BUCKETS": "50",
        "TRACK_SPARK_MASTER": "local[2]",
        "TRACK_DRIVER_MEMORY": "2g",
        "TRACK_EXECUTOR_MEMORY": "1g",
        "TRACK05_SHUFFLE_PARTITIONS": "16",
    },
    "million": {
        "TRACK05_EVENTS": "1000000",
        "TRACK05_STREAM_ROWS": "100000",
        "TRACK05_ROWS_PER_SEC": "5000",
        "TRACK05_MAX_WAIT_SEC": "240",
        "TRACK05_TRIGGER_SEC": "3",
        "TRACK05_ANALYTICS_ROWS": "200000",
        "TRACK05_BATCH_CHUNK": "100000",
        "TRACK05_SOURCE_PARTS": "4",
        "TRACK05_WRITE_BUCKETS": "50",
        "TRACK_SPARK_MASTER": "local[*]",
        "TRACK_DRIVER_MEMORY": "4g",
        "TRACK_EXECUTOR_MEMORY": "2g",
        "TRACK05_SHUFFLE_PARTITIONS": "32",
    },
}


def apply_profile() -> str:
    profile = os.environ.get("TRACK05_PROFILE", "lite").lower()
    if profile not in PROFILE_PRESETS:
        raise ValueError(f"Unknown TRACK05_PROFILE={profile!r}; choose: {', '.join(PROFILE_PRESETS)}")
    for key, value in PROFILE_PRESETS[profile].items():
        os.environ.setdefault(key, value)
    return profile


PROFILE = apply_profile()

TARGET_EVENTS = int(os.environ["TRACK05_EVENTS"])
STREAM_ROWS = int(os.environ["TRACK05_STREAM_ROWS"])
ROWS_PER_SECOND = int(os.environ["TRACK05_ROWS_PER_SEC"])
SOURCE_PARTITIONS = int(os.environ["TRACK05_SOURCE_PARTS"])
WRITE_BUCKETS = int(os.environ["TRACK05_WRITE_BUCKETS"])
MAX_WAIT_SEC = int(os.environ["TRACK05_MAX_WAIT_SEC"])
TRIGGER_SEC = int(os.environ["TRACK05_TRIGGER_SEC"])
ANALYTICS_ROWS = int(os.environ["TRACK05_ANALYTICS_ROWS"])
BATCH_CHUNK = int(os.environ["TRACK05_BATCH_CHUNK"])
SHUFFLE_PARTITIONS = int(os.environ["TRACK05_SHUFFLE_PARTITIONS"])
DATA_DIR = os.environ.get("TRACK05_DATA_DIR", "/tmp/track05_million_lab")

BRONZE_DIR = f"{DATA_DIR}/bronze"
BRONZE_PARTITIONED_DIR = f"{DATA_DIR}/bronze_partitioned"
CHECKPOINT_DIR = f"{DATA_DIR}/_checkpoint"

VIP_SHARE = 0.90
COLD_CUSTOMERS = 500
SALT_BUCKETS = 8


def count_parquet_files(root: Path) -> int:
    return sum(1 for _ in root.rglob("*.parquet"))


def build_track05_spark(phase: str) -> SparkSession:
    master = os.environ["TRACK_SPARK_MASTER"]
    builder = (
        SparkSession.builder.appName(f"curriculum-track-05-{PROFILE}-{phase}")
        .master(master)
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.driver.memory", os.environ["TRACK_DRIVER_MEMORY"])
        .config("spark.executor.memory", os.environ["TRACK_EXECUTOR_MEMORY"])
        .config("spark.ui.enabled", os.environ.get("TRACK_SPARK_UI", "true"))
        .config("spark.ui.bindAddress", "0.0.0.0")
        .config("spark.ui.port", os.environ.get("TRACK_UI_PORT", "4040"))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.streaming.metricsEnabled", "true")
    )
    return builder.getOrCreate()


def safe_stop_query(query) -> None:
    try:
        if query.isActive:
            query.stop()
    except Exception as exc:
        print(f"WARN: query.stop() failed (JVM may have exited): {exc}")


def restart_spark(spark: SparkSession, phase: str) -> SparkSession:
    section(f"Restarting Spark — fresh JVM ({phase})")
    try:
        spark.stop()
    except Exception as exc:
        print(f"WARN: spark.stop() failed: {exc}")
    time.sleep(3)
    return build_track05_spark(phase)


def _customer_id_col(value_col):
    return F.when(
        (value_col % 100) < int(VIP_SHARE * 100), F.lit("VIP")
    ).otherwise(F.concat(F.lit("C-"), (value_col % COLD_CUSTOMERS).cast("string")))


def shape_events_stream(stream_df: DataFrame) -> DataFrame:
    return (
        stream_df.withColumn("customer_id", _customer_id_col(F.col("value")))
        .withColumn("amount", (F.col("value") % 100).cast("double"))
        .withColumn(
            "country",
            F.element_at(
                F.array(F.lit("US"), F.lit("UK"), F.lit("SG")),
                (F.col("value") % 3 + 1).cast("int"),
            ),
        )
        .withColumn("bucket", (F.col("value") % WRITE_BUCKETS).cast("int"))
        .withColumnRenamed("timestamp", "event_time")
        .select("event_time", "customer_id", "country", "amount", "bucket")
    )


def shape_events_batch(ids_df: DataFrame) -> DataFrame:
    """Batch path: id column drives the same skew pattern as the stream."""
    return (
        ids_df.withColumn("customer_id", _customer_id_col(F.col("id")))
        .withColumn("amount", (F.col("id") % 100).cast("double"))
        .withColumn(
            "country",
            F.element_at(
                F.array(F.lit("US"), F.lit("UK"), F.lit("SG")),
                (F.col("id") % 3 + 1).cast("int"),
            ),
        )
        .withColumn("bucket", (F.col("id") % WRITE_BUCKETS).cast("int"))
        .withColumn("event_time", F.expr("timestamp_millis(1700000000000L + id * 1000)"))
        .select("event_time", "customer_id", "country", "amount", "bucket")
    )


def customer_dim(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([("VIP", "Platinum")], ["customer_id", "tier"]).unionByName(
        spark.range(0, COLD_CUSTOMERS).select(
            F.concat(F.lit("C-"), F.col("id").cast("string")).alias("customer_id"),
            F.lit("Standard").alias("tier"),
        )
    )


def reset_scratch() -> None:
    path = Path(DATA_DIR)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    Path(BRONZE_DIR).mkdir(parents=True, exist_ok=True)


def collect_stage_metrics(spark, label: str) -> dict:
    status = spark.sparkContext.statusTracker()
    stages = []
    for stage_id in range(400):
        info = status.getStageInfo(stage_id)
        if info is None:
            break
        stages.append(
            {
                "stage_id": info.stageId,
                "num_tasks": info.numTasks,
                "num_completed_tasks": info.numCompletedTasks,
                "num_failed_tasks": info.numFailedTasks,
            }
        )
    return {
        "label": label,
        "active_jobs": len(status.getActiveJobsIds()),
        "stage_count": len(stages),
        "recent_stages": stages[-3:],
    }


def write_bronze_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    batch_df.coalesce(1).write.mode("append").parquet(BRONZE_DIR)


def safe_last_progress(query) -> dict | None:
    try:
        if not query.isActive:
            return None
        return query.lastProgress or None
    except Exception:
        return None


def streaming_query_exception(query) -> str | None:
    try:
        exc = query.exception()
        return str(exc) if exc else None
    except Exception:
        return "JVM unreachable during streaming"


def lab_a_streaming_demo(spark: SparkSession) -> dict:
    """Stream a bounded number of rows — teaches checkpoint/watermark/micro-batches."""
    section("Lab A1 — streaming demo (rate source → bronze + checkpoint)")
    est_sec = max(1, STREAM_ROWS // max(1, ROWS_PER_SECOND))
    print(
        f"stream_target={STREAM_ROWS:,}  rows_per_sec={ROWS_PER_SECOND:,}  "
        f"trigger={TRIGGER_SEC}s  max_wait={MAX_WAIT_SEC}s  (~{est_sec}s expected)"
    )
    print(
        "Streaming teaches Track 03 semantics. Remaining rows (if any) are batch-appended in Lab A2."
    )

    stream_df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", ROWS_PER_SECOND)
        .option("numPartitions", SOURCE_PARTITIONS)
        .load()
    )
    events = shape_events_stream(stream_df).withWatermark("event_time", "30 seconds")

    query = (
        events.writeStream.foreachBatch(write_bronze_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=f"{TRIGGER_SEC} seconds")
        .start()
    )

    print(f"\n{'batch':>6} | {'input_rows':>12} | {'rows/sec':>12} | {'total_in':>14}")
    print("-" * 56)

    ingested = 0
    last_batch_id = -1
    started = time.time()
    deadline = started + MAX_WAIT_SEC
    poll_sec = max(TRIGGER_SEC, 3)

    while ingested < STREAM_ROWS and time.time() < deadline:
        time.sleep(poll_sec)
        failure = streaming_query_exception(query)
        if failure:
            safe_stop_query(query)
            raise RuntimeError(f"Streaming query failed: {failure}")

        progress = safe_last_progress(query)
        if progress is None:
            if not query.isActive:
                break
            continue

        batch_id = progress.get("batchId")
        if batch_id is None or batch_id <= last_batch_id:
            continue

        batch_rows = int(progress.get("numInputRows") or 0)
        ingested += batch_rows
        last_batch_id = batch_id
        print(
            f"{batch_id:>6} | {batch_rows:>12,} | "
            f"{progress.get('processedRowsPerSecond', 0.0):>12,.0f} | {ingested:>14,}"
        )

    safe_stop_query(query)
    elapsed = time.time() - started
    print(
        f"\nStreaming demo done: ≈{ingested:,} rows in ~{elapsed:.0f}s "
        f"(checkpoint={Path(CHECKPOINT_DIR).exists()})"
    )
    return {"stream_rows": ingested, "elapsed_sec": round(elapsed, 1)}


def lab_a_batch_fill(spark: SparkSession, stream_rows: int) -> dict:
    """Append remaining rows in bounded chunks — reaches TARGET_EVENTS without streaming OOM."""
    remaining = TARGET_EVENTS - stream_rows
    if remaining <= 0:
        print(f"\nLab A2 skipped — stream rows ({stream_rows:,}) already meet target ({TARGET_EVENTS:,})")
        return {"batch_rows": 0, "elapsed_sec": 0.0}

    section(f"Lab A2 — batch fill ({remaining:,} rows in chunks of {BATCH_CHUNK:,})")
    print("Batch fill simulates bulk bronze landing (Kafka catch-up / backfill) without driver OOM.")

    written = 0
    started = time.time()
    id_base = stream_rows

    while written < remaining:
        chunk = min(BATCH_CHUNK, remaining - written)
        start_id = id_base + written
        end_id = start_id + chunk
        chunk_df = shape_events_batch(spark.range(start_id, end_id))
        chunk_df.coalesce(1).write.mode("append").parquet(BRONZE_DIR)
        written += chunk
        print(f"  chunk appended: {chunk:,} rows  ({written:,}/{remaining:,} batch fill)")

    elapsed = time.time() - started
    print(f"Batch fill done: {written:,} rows in ~{elapsed:.0f}s")
    return {"batch_rows": written, "elapsed_sec": round(elapsed, 1)}


def load_bronze_for_analytics(spark: SparkSession, ingested: int) -> DataFrame:
    bronze = spark.read.parquet(BRONZE_DIR)
    if ingested <= ANALYTICS_ROWS:
        print(f"Analytics on full bronze ({ingested:,} rows)")
        return bronze
    fraction = min(1.0, ANALYTICS_ROWS / ingested)
    print(
        f"Analytics sample: {ANALYTICS_ROWS:,} cap from {ingested:,} total "
        f"(fraction={fraction:.4f}) — skew preserved"
    )
    return bronze.sample(withReplacement=False, fraction=fraction, seed=42).limit(ANALYTICS_ROWS)


def lab_b_partitions_small_files(spark: SparkSession, ingested: int) -> dict:
    section("Lab B — partitions & small files")
    stream_files = count_parquet_files(Path(BRONZE_DIR))
    print(f"bronze rows (stream + batch, no recount): {ingested:,}")
    print(f"parquet files on disk: {stream_files}")
    print("Lesson: micro-batch + chunked writes → many files; read parallelism follows file layout.")

    work = load_bronze_for_analytics(spark, ingested)
    print(f"partitions on analytics dataset: {work.rdd.getNumPartitions()}")

    section("Lab B — partitionBy small-file pattern")
    work.write.mode("overwrite").partitionBy("bucket").parquet(BRONZE_PARTITIONED_DIR)
    partitioned = spark.read.parquet(BRONZE_PARTITIONED_DIR)
    part_files = count_parquet_files(Path(BRONZE_PARTITIONED_DIR))
    print(f"files after partitionBy({WRITE_BUCKETS}): {part_files}")
    print(f"partitions after read: {partitioned.rdd.getNumPartitions()}")
    print(f"after coalesce(8): {partitioned.coalesce(8).rdd.getNumPartitions()}")
    return {
        "rows_ingested": ingested,
        "analytics_rows_cap": ANALYTICS_ROWS,
        "stream_files": stream_files,
        "partitioned_files": part_files,
    }


def lab_c_plans_joins_windows(spark: SparkSession, ingested: int) -> None:
    section("Lab C — plans, broadcast join, window (Track 02)")
    bronze = load_bronze_for_analytics(spark, ingested)
    dim = customer_dim(spark)

    print("explain() — default join plan:")
    print(bronze.join(dim, "customer_id", "left")._jdf.queryExecution().toString())

    section("broadcast join → spend by country")
    (
        bronze.join(F.broadcast(dim), "customer_id", "inner")
        .groupBy("country")
        .agg(F.sum("amount").alias("total_amount"), F.count(F.lit(1)).alias("events"))
        .orderBy("country")
        .show(truncate=False)
    )

    section("window — rank customers by spend")
    per_customer = bronze.groupBy("customer_id").agg(F.sum("amount").alias("spend"))
    ranked = per_customer.withColumn("rank", F.rank().over(Window.orderBy(F.col("spend").desc())))
    print("Top spenders (VIP should rank #1):")
    ranked.orderBy("rank").show(10, truncate=False)


def lab_d_war_room(spark: SparkSession, ingested: int) -> dict:
    section("Lab D — war room: naive vs salted skewed join (Track 04)")
    bronze = load_bronze_for_analytics(spark, ingested)
    dim = customer_dim(spark)

    print("Run A — naive join:")
    t0 = time.time()
    naive = bronze.join(dim, "customer_id").groupBy("tier").agg(F.sum("amount").alias("total"))
    naive_rows = naive.count()
    metrics_a = collect_stage_metrics(spark, "naive_join")
    elapsed_a = time.time() - t0
    naive.show(truncate=False)
    print(f"naive: rows={naive_rows}, elapsed_sec={elapsed_a:.2f}")

    print("\nRun B — salted join:")
    t1 = time.time()
    salted_fact = bronze.withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int")).withColumn(
        "join_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
    )
    salts = spark.range(0, SALT_BUCKETS).withColumnRenamed("id", "salt")
    salted_dim = dim.crossJoin(salts).withColumn(
        "join_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
    )
    salted = (
        salted_fact.join(salted_dim, "join_key")
        .groupBy("tier")
        .agg(F.sum("amount").alias("total"))
    )
    salted_rows = salted.count()
    metrics_b = collect_stage_metrics(spark, "salted_join")
    elapsed_b = time.time() - t1
    salted.show(truncate=False)
    print(f"salted: rows={salted_rows}, elapsed_sec={elapsed_b:.2f}")

    summary = {
        "profile": PROFILE,
        "events_target": TARGET_EVENTS,
        "events_ingested": ingested,
        "analytics_rows_cap": ANALYTICS_ROWS,
        "naive_elapsed_sec": round(elapsed_a, 2),
        "salted_elapsed_sec": round(elapsed_b, 2),
        "speedup_x": round(elapsed_a / elapsed_b, 2) if elapsed_b else None,
        "metrics_a": metrics_a,
        "metrics_b": metrics_b,
    }
    section("War-room summary")
    print(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    banner("TRACK 05 — MILLION-EVENT LAB")
    print(
        f"profile={PROFILE}  target={TARGET_EVENTS:,}  stream={STREAM_ROWS:,}  "
        f"analytics_cap={ANALYTICS_ROWS:,}\n"
        f"master={os.environ['TRACK_SPARK_MASTER']}  driver={os.environ['TRACK_DRIVER_MEMORY']}  "
        f"Spark UI: http://localhost:14040/\n"
    )
    if PROFILE == "lite":
        print(
            "lite profile: streaming demo + batch fill. For 1M rows use "
            "TRACK05_PROFILE=million on a machine with 32 GB+ RAM.\n"
        )

    reset_scratch()

    spark = build_track05_spark("stream")
    stream_result = lab_a_streaming_demo(spark)
    stream_rows = stream_result["stream_rows"]
    if stream_rows == 0:
        raise RuntimeError("Streaming demo produced 0 rows — lower TRACK05_ROWS_PER_SEC or check logs.")

    spark = restart_spark(spark, "batch")
    batch_result = lab_a_batch_fill(spark, stream_rows)
    ingested = stream_rows + batch_result["batch_rows"]

    spark = restart_spark(spark, "analytics")
    partitions = lab_b_partitions_small_files(spark, ingested)
    lab_c_plans_joins_windows(spark, ingested)
    war_room = lab_d_war_room(spark, ingested)

    section("TRACK 05 — combined scorecard")
    print(
        json.dumps(
            {
                "profile": PROFILE,
                "ingest": {
                    "target": TARGET_EVENTS,
                    "total": ingested,
                    "stream": stream_result,
                    "batch": batch_result,
                },
                "lesson_01_partitions": partitions,
                "lesson_04_war_room": {
                    "naive_sec": war_room["naive_elapsed_sec"],
                    "salted_sec": war_room["salted_elapsed_sec"],
                    "speedup_x": war_room["speedup_x"],
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_track(main, "TRACK 05 — MILLION-EVENT LAB"))
