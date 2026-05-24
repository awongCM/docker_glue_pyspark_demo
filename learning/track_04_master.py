#!/usr/bin/env python3
"""Track 04 — Master: skew war-room drill + metrics collection (lightweight)."""
import json
import time

from pyspark.sql import functions as F

from _common import banner, build_spark, run_track, section


def collect_stage_metrics(spark, label: str) -> dict:
    """Snapshot Spark listener metrics after an action (driver-side)."""
    status = spark.sparkContext.statusTracker()
    stages = []
    stage_id = 0
    while stage_id < 200:
        info = status.getStageInfo(stage_id)
        if info is None:
            break
        stages.append(
            {
                "stage_id": info.stageId,
                "name": info.name,
                "num_tasks": info.numTasks,
                "num_completed_tasks": info.numCompletedTasks,
                "num_failed_tasks": info.numFailedTasks,
            }
        )
        stage_id += 1
    return {
        "label": label,
        "active_jobs": len(status.getActiveJobsIds()),
        "stage_count": len(stages),
        "recent_stages": stages[-3:],
    }


def main() -> None:
    spark = build_spark("curriculum-track-04-master", shuffle_partitions=16)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    banner("TRACK 04 — MASTER (skew war-room)")

    section("Build skewed fact + dimension (hot key = 'VIP')")
    fact = (
        spark.range(0, 20000)
        .withColumn("customer_id", F.when(F.col("id") < 18000, "VIP").otherwise(F.concat(F.lit("C-"), (F.col("id") % 500).cast("string"))))
        .withColumn("amount", (F.col("id") % 100).cast("double"))
    )
    dim = spark.createDataFrame(
        [("VIP", "Platinum"), ("C-1", "Standard")],
        ["customer_id", "tier"],
    ).unionByName(
        spark.range(1, 500).select(
            F.concat(F.lit("C-"), F.col("id").cast("string")).alias("customer_id"),
            F.lit("Standard").alias("tier"),
        )
    )

    section("Run A — naive join (observe shuffle / stragglers in Spark UI)")
    t0 = time.time()
    naive = fact.join(dim, "customer_id").groupBy("tier").sum("amount")
    naive_count = naive.count()
    metrics_a = collect_stage_metrics(spark, "naive_join")
    elapsed_a = time.time() - t0
    print(f"naive result rows: {naive_count}, elapsed_sec: {elapsed_a:.2f}")
    naive.show(5)

    section("Run B — AQE + salting pattern (same query, skew mitigations on)")
    t1 = time.time()
    salted_fact = fact.withColumn("salt", (F.rand() * 8).cast("int"))
    salts = spark.range(0, 8).withColumnRenamed("id", "salt")
    salted_dim = dim.crossJoin(salts)
    improved = (
        salted_fact.withColumn("join_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt")))
        .join(
            salted_dim.withColumn("join_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))),
            "join_key",
        )
        .groupBy("tier")
        .agg(F.sum("amount").alias("total_amount"))
    )
    improved_count = improved.count()
    metrics_b = collect_stage_metrics(spark, "salted_join")
    elapsed_b = time.time() - t1
    print(f"improved result rows: {improved_count}, elapsed_sec: {elapsed_b:.2f}")
    improved.show(5)

    section("War-room summary (paste into runbook)")
    summary = {
        "naive_elapsed_sec": round(elapsed_a, 2),
        "improved_elapsed_sec": round(elapsed_b, 2),
        "spark_conf": {
            "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
            "skew_join": spark.conf.get("spark.sql.adaptive.skewJoin.enabled"),
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        },
        "metrics_a": metrics_a,
        "metrics_b": metrics_b,
    }
    print(json.dumps(summary, indent=2))
    print(
        "\nObserve in Spark UI (http://localhost:4040 while job runs, or :18080 history):\n"
        "  - Stages tab: task duration skew on Run A\n"
        "  - SQL tab: Exchange nodes, AQE plan changes on Run B\n"
    )


if __name__ == "__main__":
    raise SystemExit(run_track(main, "TRACK 04 — MASTER"))
