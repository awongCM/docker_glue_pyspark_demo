"""Shared Spark session helper for curriculum track demos (low memory, unique app names)."""
import os
import sys
from pyspark.sql import SparkSession


def build_spark(app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    """Lightweight session for parallel track runs (default local[1])."""
    master = os.environ.get("TRACK_SPARK_MASTER", "local[1]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("TRACK_DRIVER_MEMORY", "512m"))
        .config("spark.ui.enabled", os.environ.get("TRACK_SPARK_UI", "true"))
        .config("spark.ui.bindAddress", "0.0.0.0")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
    )
    ui_port = os.environ.get("TRACK_UI_PORT", "4040")
    builder = builder.config("spark.ui.port", ui_port)
    return builder.getOrCreate()


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72 + "\n", flush=True)


def section(title: str) -> None:
    print(f"\n--- {title} ---\n", flush=True)


def run_track(main_fn, track_name: str) -> int:
    try:
        main_fn()
        banner(f"{track_name} COMPLETE")
        return 0
    except Exception as exc:
        print(f"\n[ERROR] {track_name} failed: {exc}", file=sys.stderr, flush=True)
        import traceback

        traceback.print_exc()
        return 1
    finally:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active:
            active.stop()
