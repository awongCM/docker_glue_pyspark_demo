#!/usr/bin/env python3
"""Track 01 — Basic: partitions, lazy evaluation, many-files mini-lab."""
from pyspark.sql import functions as F

from _common import banner, build_spark, run_track, section


def main() -> None:
    spark = build_spark("curriculum-track-01-basic", shuffle_partitions=4)
    banner("TRACK 01 — BASIC")

    # Lab 1 — SparkSession + plain-shaped data (matches repo plain pipeline)
    section("Lab 1 — DataFrame from plain events")
    rows = [
        (1, "Alice", 50),
        (2, "Bob", 75),
        (3, "Charlie", 120),
        (4, "Diana", 30),
        (5, "Eve", 200),
    ]
    df = spark.createDataFrame(rows, ["id", "name", "amount"])
    print("Schema:")
    df.printSchema()
    print("Sample rows:")
    df.show()

    # Lab 2 — partitions + lazy vs actions
    section("Lab 2 — Partitions & lazy evaluation")
    repartitioned = df.repartition(8, "id")
    print(f"defaultParallelism: {spark.sparkContext.defaultParallelism}")
    print(f"rdd partitions (after repartition(8)): {repartitioned.rdd.getNumPartitions()}")
    print("Calling count() — triggers a job (watch driver logs / Spark UI Jobs tab)")
    print(f"row count = {repartitioned.count()}")
    print("filter is lazy until show():")
    high = repartitioned.filter(F.col("amount") > 50)
    high.show()

    # Mini Lab 4 — many small partitions / files intuition
    section("Mini Lab 4 — many files → coalesce")
    many = spark.range(0, 500).withColumn("bucket", F.col("id") % 50)
    out_dir = "/tmp/track01_many_files"
    many.write.mode("overwrite").partitionBy("bucket").parquet(out_dir)
    read_back = spark.read.parquet(out_dir)
    print(f"partitions after read (many files): {read_back.rdd.getNumPartitions()}")
    coalesced = read_back.coalesce(4)
    print(f"partitions after coalesce(4): {coalesced.rdd.getNumPartitions()}")
    print("aggregated by bucket (4 shuffle partitions):")
    coalesced.groupBy("bucket").count().orderBy("bucket").show(8)


if __name__ == "__main__":
    raise SystemExit(run_track(main, "TRACK 01 — BASIC"))
