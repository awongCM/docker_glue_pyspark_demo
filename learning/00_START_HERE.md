# PySpark curriculum — run all 4 tracks in parallel

Executable demos for **Basic**, **Intermediate**, **Advanced**, and **Master** tracks. Each track uses a **unique Spark app name**, **`local[1]`**, and **small data** so four processes can run without OOM.

## Prerequisites

```bash
./scripts/start-containers.bash
cd terraform && tflocal apply -auto-approve   # only for medallion / Kafka labs
```

Restart compose after pulling this repo (mounts `./learning`):

```bash
docker-compose up -d glue-pyspark jupyterlab
```

## Option A — One command (staggered parallel, recommended)

```bash
chmod +x learning/run_all_tracks_parallel.sh
./learning/run_all_tracks_parallel.sh
```

- Starts tracks **5 seconds apart** (override: `TRACK_STAGGER_SEC=8`)
- Logs: `learning/logs/track_01.log` … `track_04.log`
- Tail: `tail -f learning/logs/track_*.log`

## Option B — Four terminals (manual parallel)

```bash
docker exec -it glue-pyspark-poc bash -lc \
  'cd /app/learning && TRACK_SPARK_MASTER=local[1] poetry run python track_01_basic.py'
# … repeat for track_02_intermediate.py, track_03_advanced.py, track_04_master.py
```

Wait ~10s between starts if RAM is tight.

## Option C — Jupyter (4 tabs, best for observation)

1. Open http://localhost:8888/ (token: `test`)
2. Upload or open notebooks under `learning/` (or run `.py` cells via `%run track_01_basic.py`)
3. Run one track per tab — **do not** use `local[*]` in all tabs at once

## Option D — Make targets

```bash
make -C learning tracks      # orchestrator
make -C learning track1      # single track
make -C learning medallion   # bronze→silver→gold extension
```

## URLs to watch

| URL | What to observe |
|-----|-----------------|
| http://localhost:8888/ | Jupyter — interactive reruns |
| http://localhost:4040/ | Live Spark UI (while a track JVM is running) |
| http://localhost:18080/ | Spark History (if enabled in image) |
| `learning/logs/track_*.log` | Console output from orchestrator |

## Per-track: what to observe

| Track | File | Watch for |
|-------|------|-----------|
| **01 Basic** | `track_01_basic.py` | `repartition` partition count, lazy `count()` vs `show()`, `coalesce` after many Parquet files |
| **02 Intermediate** | `track_02_intermediate.py` | `explain()` plan, `BroadcastHashJoin` in physical plan, window running totals |
| **03 Advanced** | `track_03_advanced.py` | Skewed join counts, bad vs fixed filter order, streaming console batches + checkpoint dir |
| **04 Master** | `track_04_master.py` | JSON war-room summary, stage timing skew (Spark UI Stages tab) |

## Medallion extension (Track 1 — Lab 3)

Full plain pipeline (needs LocalStack + Kafka):

```bash
./learning/run_medallion_plain.sh
```

Or step-by-step from README:

```bash
docker exec glue-pyspark-poc python /app/scripts/generate_plain_events.py 10
docker exec glue-pyspark-poc poetry run python /app/plain/bronze_job.py   # Ctrl+C after ~15s
docker exec glue-pyspark-poc poetry run python /app/plain/silver_job.py
docker exec glue-pyspark-poc poetry run python /app/plain/gold_job.py
```

## Troubleshooting

- **`learning/ not mounted`** → `docker-compose up -d glue-pyspark`
- **OOM with 4 parallel** → increase `TRACK_STAGGER_SEC` or run tracks sequentially
- **Medallion fails** → run `tflocal apply` and check Kafka at `kafka:29092` from container
