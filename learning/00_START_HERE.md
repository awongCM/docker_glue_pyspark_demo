# PySpark curriculum — run all 4 tracks in parallel

Executable demos for **Basic**, **Intermediate**, **Advanced**, and **Master** tracks. Each track uses a **unique Spark app name**, `**local[1]`**, and **small data** so four processes can run without OOM.

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

## Local RAM limits (Docker on a laptop)

Spark in Docker shares RAM with your OS, IDE, and browser. **Local mode is not a cluster** — the driver and executors all compete for the same physical memory.

| Symptom | Likely cause | What to do |
|---------|--------------|------------|
| `Py4JNetworkError: Answer from Java side is empty` | JVM OOM or crash | Lower data size, use `local[1]`, reduce driver memory *or* raise Docker Desktop memory limit |
| `Current batch is falling behind` | Streaming ingest faster than writes | **Lower** `TRACK05_ROWS_PER_SEC`; do not raise it |
| `Connection refused` after a long job | JVM already died; Python still calling Spark | Re-run with a smaller profile; check `docker stats` during the job |
| Track 05 dies in Lab A | Streaming 1M rows on limited RAM | Use default **`TRACK05_PROFILE=lite`** (100k total, 15k streamed) |
| Four parallel tracks OOM | 4× JVM @ 512m each + overhead | Increase `TRACK_STAGGER_SEC` or run tracks one at a time (`make -C learning track1` …) |

**Rough guide (host RAM → what runs comfortably):**

| Host RAM | Tracks 01–04 (parallel) | Track 05 profile |
|----------|-------------------------|------------------|
| 8 GB | Sequential only, or parallel with `TRACK_STAGGER_SEC=10` | `lite` only |
| 16 GB | Parallel (default stagger) | `lite` or `standard` |
| 32 GB+ | Parallel + Jupyter | `standard` or `million` (raise Docker memory limit) |

**Design choices in this repo for low RAM:**

- Tracks **01–04** use `local[1]` and **512m driver** so four apps can run without OOM.
- Track **05** splits work: **short streaming demo** (Track 03 semantics) + **chunked batch fill** (scale without holding everything in the streaming driver) + **fresh JVM restarts** between phases.
- Track 01 Mini Lab 4 defaults to **100k rows** (not 1M) for the same reason — tunable via `TRACK01_ROWS` when you have headroom.

**Docker Desktop:** Settings → Resources → Memory — give the VM at least **6 GB** for Track 05 `lite`, **10 GB+** for `standard`/`million`.

Production clusters have separate driver/executor nodes and GB–TB of aggregate memory; **concepts transfer, absolute numbers do not**. Use Spark UI Stages (task skew, duration) and `explain()` — those behave the same at any scale.

## Option C — Jupyter (4 tabs, best for observation)

1. Open [http://localhost:8888/](http://localhost:8888/) (token: `test`)
2. Upload or open notebooks under `learning/` (or run `.py` cells via `%run track_01_basic.py`)
3. Run one track per tab — **do not** use `local[*]` in all tabs at once

## Option D — Make targets

```bash
make -C learning tracks      # orchestrator
make -C learning track1      # single track
make -C learning medallion   # bronze→silver→gold extension
```

## URLs to watch


| URL                                                | What to observe                              |
| -------------------------------------------------- | -------------------------------------------- |
| [http://localhost:8888/](http://localhost:8888/)   | Jupyter — interactive reruns                 |
| [http://localhost:14040/](http://localhost:14040/) | Live Spark UI from **glue-pyspark** tracks (only while a job runs) |
| [http://localhost:14041/](http://localhost:14041/) | Live Spark UI from **Jupyter** notebooks (only while a session runs) |
| [http://localhost:18080/](http://localhost:18080/) | Spark History — starts with `glue-pyspark` container (recreate compose after pull) |
| `learning/logs/track_*.log`                        | Console output from orchestrator             |


## Per-track: what to observe


| Track               | File                       | Watch for                                                                                      |
| ------------------- | -------------------------- | ---------------------------------------------------------------------------------------------- |
| **01 Basic**        | `track_01_basic.py`        | `repartition` partition count, lazy `count()` vs `show()`, `coalesce` after many Parquet files |
| **02 Intermediate** | `track_02_intermediate.py` | `explain()` plan, `BroadcastHashJoin` in physical plan, window running totals                  |
| **03 Advanced**     | `track_03_advanced.py`     | Skewed join counts, bad vs fixed filter order, streaming console batches + checkpoint dir      |
| **04 Master**       | `track_04_master.py`       | JSON war-room summary, stage timing skew (Spark UI Stages tab)                                 |


## Track 05 — million-event lab (run SOLO, not in the parallel orchestrator)

One run that replays all four lessons. Uses **RAM-aware profiles** — defaults to **`lite`** so it completes on a laptop.

| Profile | Total rows | Streaming demo | Spark | Driver | RAM hint |
|---------|------------|----------------|-------|--------|----------|
| **lite** (default) | 100k | 15k streamed + 85k batch chunks | `local[1]` | 1g | 8–16 GB |
| **standard** | 500k | 50k + batch fill | `local[2]` | 2g | 16 GB+ |
| **million** | 1M | 100k + batch fill | `local[*]` | 4g | 32 GB+ |

```bash
./learning/run_track05_million.sh                              # lite (default)
TRACK05_PROFILE=standard ./learning/run_track05_million.sh     # more scale
TRACK05_PROFILE=million ./learning/run_track05_million.sh      # needs RAM
```

**Do not** run alongside the four parallel tracks.

Labs: **A1** streaming (checkpoint/watermark) → **A2** batch fill to target → **B** small files → **C** plans/joins → **D** skew war-room.

If streaming falls behind or JVM dies: stay on `lite`, or lower `TRACK05_ROWS_PER_SEC`. Do not raise throughput on a memory-constrained machine. See **Local RAM limits** (above) for symptoms and Docker memory settings.


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
- **Track 05 JVM crash** → stay on `TRACK05_PROFILE=lite`; see **Local RAM limits** above
- **Medallion fails** → run `tflocal apply` and check Kafka at `kafka:29092` from container
- **Spark UI won't load** → use **14040** (not 4040): macOS often has SSH listening on 4040/4041. Recreate: `docker-compose up -d --force-recreate glue-pyspark`. Start a track while opening [http://localhost:14040/](http://localhost:14040/). **18080** works anytime after recreate.

