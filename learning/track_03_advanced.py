#!/usr/bin/env python3
"""Track 03 — Advanced: bad vs fixed job, rate stream + checkpoint, skew demo."""
import shutil
from pathlib import Path

from pyspark.sql import functions as F

from _common import banner, build_spark, run_track, section


def main() -> None:
    spark = build_spark("curriculum-track-03-advanced", shuffle_partitions=8)
    banner("TRACK 03 — ADVANCED")

    # Skewed keys: 90% of rows on key=1
    section("Skew — hot key vs salted join")
    skewed = spark.range(0, 1000).withColumn(
        "key", F.when(F.col("id") < 900, F.lit(1)).otherwise(F.col("id") % 100)
    )
    dim = spark.range(0, 100).withColumnRenamed("id", "key").withColumn(
        "label", F.concat(F.lit("dim-"), F.col("key").cast("string"))
    )
    print("Naive join partition count (shuffle on skewed key):")
    naive = skewed.join(dim, "key")
    print(f"naive join partitions: {naive.rdd.getNumPartitions()}")
    print(f"naive count (triggers shuffle): {naive.count()}")

    salt_buckets = 8
    salted_left = skewed.withColumn(
        "salt", (F.rand() * salt_buckets).cast("int")
    ).withColumn("salted_key", F.concat(F.col("key"), F.lit("_"), F.col("salt")))
    salts = spark.range(0, salt_buckets).withColumnRenamed("id", "salt")
    salted_right = dim.crossJoin(salts).withColumn(
        "salted_key", F.concat(F.col("key"), F.lit("_"), F.col("salt"))
    )
    salted = salted_left.join(salted_right, "salted_key").drop("salt", "salted_key")
    print(f"salted join count: {salted.count()}")

    section("Bad job vs fixed — filter before shuffle")
    events = spark.range(0, 5000).withColumn(
        "status", F.when(F.col("id") % 100 == 0, "BAD").otherwise("OK")
    )
    bad_plan = events.alias("a").join(events.alias("b"), "id").filter(
        F.col("a.status") == "BAD"
    )
    print("BAD: join before filter — scans full dataset in shuffle:")
    print(f"bad count = {bad_plan.count()}")

    fixed = events.filter(F.col("status") == "BAD").alias("a").join(
        events.filter(F.col("status") == "BAD").alias("b"), F.col("a.id") == F.col("b.id")
    )
    print(f"FIXED: pre-filter both sides — count = {fixed.count()}")

    section("Structured streaming — rate source + checkpoint")
    cp = Path("/tmp/track03_stream_checkpoint")
    if cp.exists():
        shutil.rmtree(cp)
    stream_df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .withWatermark("timestamp", "10 seconds")
        .groupBy(F.window("timestamp", "5 seconds"))
        .count()
    )
    query = (
        stream_df.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", str(cp))
        .start()
    )
    import time

    print("Streaming for ~8s (console sink) — watch batch logs below:")
    time.sleep(8)
    query.stop()
    print(f"checkpoint dir exists: {cp.exists()}")


if __name__ == "__main__":
    raise SystemExit(run_track(main, "TRACK 03 — ADVANCED"))
