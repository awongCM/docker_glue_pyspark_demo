#!/usr/bin/env python3
"""Track 02 — Intermediate: explain(), repartition/coalesce, window, broadcast join."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from _common import banner, build_spark, run_track, section


def main() -> None:
    spark = build_spark("curriculum-track-02-intermediate", shuffle_partitions=8)
    banner("TRACK 02 — INTERMEDIATE")

    customers = spark.createDataFrame(
        [(1, "Alice", "US"), (2, "Bob", "UK"), (3, "Charlie", "US")],
        ["customer_id", "name", "country"],
    )
    orders = spark.createDataFrame(
        [
            (101, 1, 50.0),
            (102, 1, 75.0),
            (103, 2, 120.0),
            (104, 3, 30.0),
            (105, 99, 999.0),  # orphan for join demo
        ],
        ["order_id", "customer_id", "amount"],
    )

    section("explain() — sort-merge join (default)")
    joined = orders.join(customers, "customer_id", "left")
    print(joined._jdf.queryExecution().toString())

    section("repartition vs coalesce")
    wide = orders.repartition(16, "customer_id")
    print(f"after repartition(16): {wide.rdd.getNumPartitions()} partitions")
    narrow = wide.coalesce(2)
    print(f"after coalesce(2): {narrow.rdd.getNumPartitions()} partitions")

    section("window — running total per customer")
    w = Window.partitionBy("customer_id").orderBy("order_id")
    windowed = (
        joined.withColumn("running_total", F.sum("amount").over(w))
        .select("customer_id", "name", "order_id", "amount", "running_total")
        .orderBy("customer_id", "order_id")
    )
    windowed.show()

    section("broadcast join hint (small dimension)")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
    broadcasted = orders.join(F.broadcast(customers), "customer_id", "inner")
    print("Physical plan (look for BroadcastHashJoin):")
    print(broadcasted._jdf.queryExecution().executedPlan().toString())
    broadcasted.groupBy("country").agg(F.sum("amount").alias("total")).show()


if __name__ == "__main__":
    raise SystemExit(run_track(main, "TRACK 02 — INTERMEDIATE"))
