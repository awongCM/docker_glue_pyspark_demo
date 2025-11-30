# Test Execution Quick Reference

## 🐳 Run Tests in Docker (Required)

Tests **MUST** run inside the Docker container because they require the `awsglue` module.

### Quick Commands

```bash
# 1. Start containers first
./scripts/start-containers.bash

# 2. Run tests
./scripts/run-tests-docker.bash all        # All tests
./scripts/run-tests-docker.bash plain      # Plain pipeline only
./scripts/run-tests-docker.bash po         # Purchase order only
./scripts/run-tests-docker.bash bronze     # All bronze layers
./scripts/run-tests-docker.bash silver     # All silver layers
./scripts/run-tests-docker.bash gold       # All gold layers
./scripts/run-tests-docker.bash fast       # Skip coverage (faster)
./scripts/run-tests-docker.bash coverage   # Generate HTML report
./scripts/run-tests-docker.bash shell      # Open interactive shell
```

## 🔧 Manual Testing

```bash
# Method 1: Use helper script
./scripts/run-tests-docker.bash shell
cd /app
poetry run pytest -v

# Method 2: Direct docker exec
docker exec -it glue-pyspark-poc bash
cd /app
poetry run pytest tests/plain/
```

## 📊 Common pytest Commands (Inside Container)

```bash
# All tests with coverage
poetry run pytest -v

# Specific file
poetry run pytest tests/plain/test_bronze_job.py -v

# Specific test class
poetry run pytest tests/plain/test_silver_job.py::TestSilverJobDataTransformation -v

# Specific test function
poetry run pytest tests/plain/test_gold_job.py::TestGoldJobDynamoDBOperations::test_should_format_item_correctly_when_writing_to_dynamodb -v

# Show print statements
poetry run pytest -s

# Stop on first failure
poetry run pytest -x

# Run last failed tests
poetry run pytest --lf

# Show coverage
poetry run pytest --cov=plain --cov=purchase_order --cov-report=term-missing
```

## 🚨 Troubleshooting

### "ModuleNotFoundError: No module named 'awsglue'"
✅ **Solution**: Run tests inside the Docker container, not locally.
```bash
./scripts/run-tests-docker.bash all
```

### "Container 'glue-pyspark-poc' is not running"
✅ **Solution**: Start the containers first.
```bash
./scripts/start-containers.bash
docker ps | grep glue-pyspark-poc
```

### "poetry: command not found" inside container
✅ **Solution**: The container should have Poetry pre-installed. Rebuild if needed.
```bash
docker-compose build glue-pyspark
docker-compose up -d glue-pyspark
```

### Tests are slow
✅ **Solution**: Use fast mode (skips coverage).
```bash
./scripts/run-tests-docker.bash fast
```

### Need to debug a specific test
✅ **Solution**: Open shell and run with verbose output.
```bash
./scripts/run-tests-docker.bash shell
poetry run pytest tests/plain/test_bronze_job.py::TestBronzeJobLogging -vv -s
```

## 📁 Test Structure

```
tests/
├── conftest.py              # Shared fixtures
├── plain/
│   ├── test_bronze_job.py   # 9 tests
│   ├── test_silver_job.py   # 11 tests
│   └── test_gold_job.py     # 13 tests
└── purchase_order/
    ├── test_bronze_job.py   # 13 tests
    ├── test_silver_job.py   # 12 tests
    └── test_gold_job.py     # 15 tests
```

Total: **~83 tests**

## 📖 Full Documentation

- **Quick Start**: `TESTING.md`
- **Comprehensive Guide**: `tests/README.md`
- **Implementation Summary**: `TEST_SUITE_SUMMARY.md`
- **Test Guidelines**: `.github/prompts/unit-tester.prompt.md`
