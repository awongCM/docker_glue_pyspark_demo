# Docker Test Setup - Final Summary

## ✅ What Was Changed

### 1. Created Docker Test Runner Script
- **File**: `scripts/run-tests-docker.bash` (executable)
- **Purpose**: Run tests inside the `glue-pyspark-poc` container where `awsglue` is available
- **Commands**: `all`, `plain`, `po`, `bronze`, `silver`, `gold`, `fast`, `coverage`, `shell`, `clean`

### 2. Updated docker-compose.yaml
Added volume mounts to sync test files into the container:
```yaml
volumes:
  - ./tests:/app/tests
  - ./scripts:/app/scripts
  - ./pytest.ini:/app/pytest.ini
  - ./pyproject.toml:/app/pyproject.toml
  - ./poetry.lock:/app/poetry.lock
```

### 3. Updated Documentation
- `TESTING.md` - Emphasized Docker requirement, updated all commands
- `tests/README.md` - Added Docker-first approach, updated troubleshooting
- `TEST_QUICK_REFERENCE.md` - New quick reference card for Docker testing
- `scripts/run-tests.bash` - Added deprecation warning

## 🚀 How to Use

### Step 1: Restart Containers (Required)
Since we modified `docker-compose.yaml`, restart to pick up volume changes:

```bash
# Stop containers
docker-compose down

# Start containers (will mount new volumes)
./scripts/start-containers.bash

# Or use docker-compose directly
docker-compose up -d glue-pyspark
```

### Step 2: First-Time Setup Inside Container
```bash
# Open shell in container
./scripts/run-tests-docker.bash shell

# Install test dependencies (first time only)
cd /app
poetry install

# Verify pytest is available
poetry run pytest --version

# Exit
exit
```

### Step 3: Run Tests
```bash
# Run all tests
./scripts/run-tests-docker.bash all

# Or specific pipelines/layers
./scripts/run-tests-docker.bash plain
./scripts/run-tests-docker.bash bronze
./scripts/run-tests-docker.bash coverage
```

## 📋 Command Reference

### Docker Test Runner (PRIMARY)
```bash
./scripts/run-tests-docker.bash all        # All tests with coverage
./scripts/run-tests-docker.bash plain      # Plain pipeline only
./scripts/run-tests-docker.bash po         # Purchase order only
./scripts/run-tests-docker.bash bronze     # All bronze layers
./scripts/run-tests-docker.bash silver     # All silver layers
./scripts/run-tests-docker.bash gold       # All gold layers
./scripts/run-tests-docker.bash fast       # Skip coverage (faster)
./scripts/run-tests-docker.bash coverage   # Generate HTML report
./scripts/run-tests-docker.bash verbose    # Verbose output
./scripts/run-tests-docker.bash clean      # Remove test artifacts
./scripts/run-tests-docker.bash shell      # Interactive shell
```

### Manual Docker Commands (ALTERNATIVE)
```bash
# Open shell
docker exec -it glue-pyspark-poc bash

# Run tests inside container
cd /app
poetry run pytest -v
poetry run pytest tests/plain/ -v
poetry run pytest tests/plain/test_bronze_job.py -v
```

## 🔍 Verification Steps

### 1. Verify Container is Running
```bash
docker ps | grep glue-pyspark-poc
# Should show: glue-pyspark-poc   Up   ...
```

### 2. Verify Volumes are Mounted
```bash
docker exec -it glue-pyspark-poc ls -la /app/tests
# Should show: test files and directories
```

### 3. Verify Poetry is Available
```bash
docker exec -it glue-pyspark-poc poetry --version
# Should show: Poetry (version 1.x.x)
```

### 4. Run a Quick Test
```bash
./scripts/run-tests-docker.bash fast
# Should run tests successfully
```

## ⚠️ Important Notes

### Why Docker is Required
- Tests import from `awsglue.context` and `awsglue.dynamicframe`
- These modules are only available in the AWS Glue Docker image
- Running tests locally will fail with `ModuleNotFoundError: No module named 'awsglue'`

### Volume Mounts
The following directories/files are synced from host → container:
- `tests/` - All test files
- `scripts/` - Test runner scripts
- `pytest.ini` - Pytest configuration
- `pyproject.toml` - Dependencies and project config
- `poetry.lock` - Locked dependency versions

Any changes to these files on the host are immediately available in the container (no rebuild needed).

### Container Persistence
- Test artifacts (`htmlcov/`, `.coverage`) are created inside the container
- Due to volume mounts, they're also visible on the host machine
- Coverage reports can be opened from the host: `open htmlcov/index.html`

## 🐛 Troubleshooting

### "Container 'glue-pyspark-poc' is not running"
```bash
./scripts/start-containers.bash
# Wait for containers to be healthy
docker ps | grep glue-pyspark-poc
```

### "poetry: command not found" inside container
```bash
# Rebuild the container
docker-compose build glue-pyspark
docker-compose up -d glue-pyspark
```

### "No module named 'pytest'" inside container
```bash
# Install dependencies
./scripts/run-tests-docker.bash shell
cd /app
poetry install
exit
```

### Tests fail with "ModuleNotFoundError: No module named 'awsglue'"
This means you're running tests on the host instead of in the container.
```bash
# Wrong
poetry run pytest

# Correct
./scripts/run-tests-docker.bash all
```

### Changes to test files not reflected
If you modified test files but don't see changes:
```bash
# Verify volume mount
docker exec -it glue-pyspark-poc ls -la /app/tests

# If empty, restart containers
docker-compose down
docker-compose up -d glue-pyspark
```

## 📚 Documentation Files

- **`TEST_QUICK_REFERENCE.md`** - Quick command reference (this file)
- **`TESTING.md`** - Quick start guide with Docker focus
- **`tests/README.md`** - Comprehensive test suite documentation
- **`TEST_SUITE_SUMMARY.md`** - Complete implementation details
- **`.github/prompts/unit-tester.prompt.md`** - Test writing guidelines

## 🎯 Next Steps

1. **Restart containers** to pick up new volume mounts:
   ```bash
   docker-compose down && ./scripts/start-containers.bash
   ```

2. **Install dependencies** inside container (first time):
   ```bash
   ./scripts/run-tests-docker.bash shell
   poetry install
   exit
   ```

3. **Run tests**:
   ```bash
   ./scripts/run-tests-docker.bash all
   ```

4. **View coverage**:
   ```bash
   ./scripts/run-tests-docker.bash coverage
   open htmlcov/index.html
   ```

---

**All test infrastructure is now Docker-ready! 🐳✅**
