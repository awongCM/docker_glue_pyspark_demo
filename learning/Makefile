.PHONY: all tracks track1 track2 track3 track4 medallion logs

CONTAINER ?= glue-pyspark-poc
PY := docker exec $(CONTAINER) bash -lc 'cd /app/learning && TRACK_SPARK_MASTER=local[1] TRACK_DRIVER_MEMORY=512m poetry run python'

all: tracks

tracks:
	./learning/run_all_tracks_parallel.sh

track1:
	$(PY) track_01_basic.py

track2:
	$(PY) track_02_intermediate.py

track3:
	$(PY) track_03_advanced.py

track4:
	$(PY) track_04_master.py

medallion:
	./learning/run_medallion_plain.sh

logs:
	tail -f learning/logs/track_*.log
