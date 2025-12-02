# Unit Testing Quick Start Guide

## Important: Running Tests Inside Docker Container

⚠️ **Tests must run inside the `glue-pyspark-poc` Docker container** because they require the `awsglue` module which is only available in the AWS Glue Docker image.

## Setup

1. **Start the Docker containers:**

   ```bash
   ./scripts/start-containers.bash
   ```

2. **Verify container is running:**
   ```bash
   docker ps | grep glue-pyspark-poc
   ```

## Running Tests

### Quick Commands (Docker)

```bash
# Run all tests inside container
./scripts/run-tests-docker.bash all

# Or open a shell in the container
./scripts/run-tests-docker.bash shell
# Then run: cd /app && poetry run pytest
```

### Alternative: Local Testing (Limited)

⚠️ **Local tests will fail on imports that require `awsglue`**. Use only for testing non-Glue components.

```bash
# This will fail on awsglue imports
poetry run pytest
```

### Common Test Scenarios (Inside Docker)

```bash
# Plain pipeline only
./scripts/run-tests-docker.bash plain

# Purchase order pipeline only
./scripts/run-tests-docker.bash po

# Specific layer across both pipelines
./scripts/run-tests-docker.bash bronze
./scripts/run-tests-docker.bash silver
./scripts/run-tests-docker.bash gold

# Fast run (skip coverage)
./scripts/run-tests-docker.bash fast

# Verbose output
./scripts/run-tests-docker.bash verbose

# Open interactive shell
./scripts/run-tests-docker.bash shell
```

### Generate Coverage Report

```bash
# Generate HTML report inside container
./scripts/run-tests-docker.bash coverage

# Then open locally
open htmlcov/index.html
```

### Manual Testing Inside Container

```bash
# Method 1: Use the shell command
./scripts/run-tests-docker.bash shell
cd /app
poetry run pytest

# Method 2: Direct docker exec
docker exec -it glue-pyspark-poc bash
cd /app
poetry run pytest tests/plain/
exit
```

## Understanding Test Output

### Successful Test

```
tests/plain/test_bronze_job.py::TestBronzeJobLogging::test_should_log_message_to_cloudwatch_when_valid_message_provided PASSED
```

### Failed Test

```
tests/plain/test_silver_job.py::TestSilverJobDataTransformation::test_should_filter_records_above_threshold_when_cleaning_data FAILED

AssertionError: assert 4 == 3
Expected 3 records after filtering, got 4
```

### Coverage Summary

```
Name                         Stmts   Miss  Cover   Missing
----------------------------------------------------------
plain/bronze_job.py            45     12    73%   89-95, 101-105
plain/silver_job.py            38      8    79%   76-82
plain/gold_job.py              32      5    84%   67-71
----------------------------------------------------------
TOTAL                         115     25    78%
```

## Test Structure Overview

```
tests/
├── conftest.py          # Shared fixtures (spark_session, mocks, sample data)
├── plain/
│   ├── test_bronze_job.py   # 4 test classes, ~12 tests
│   ├── test_silver_job.py   # 4 test classes, ~15 tests
│   └── test_gold_job.py     # 4 test classes, ~14 tests
└── purchase_order/
    ├── test_bronze_job.py   # 4 test classes, ~10 tests
    ├── test_silver_job.py   # 4 test classes, ~17 tests
    └── test_gold_job.py     # 4 test classes, ~15 tests
```

## What's Tested

### Plain Pipeline

- ✅ Kafka message parsing with schema validation
- ✅ S3 read/write configuration
- ✅ Data filtering (amount < 100)
- ✅ Iceberg table creation and writes
- ✅ DynamoDB item formatting
- ✅ CloudWatch logging

### Purchase Order Pipeline

- ✅ Nested schema handling (contact_info, items array)
- ✅ Array explosion (denormalization)
- ✅ Weight quantity filtering (> 5000)
- ✅ Customer aggregation (sum, average)
- ✅ Decimal precision for monetary values
- ✅ CloudWatch logging

## Fixtures Available

Use these fixtures in your tests by adding them as function parameters:

- `spark_session` - Local SparkSession
- `mock_boto3_logs_client` - Mocked CloudWatch Logs
- `mock_boto3_s3_client` - Mocked S3
- `mock_boto3_dynamodb_client` - Mocked DynamoDB
- `plain_sample_data` - 4 sample plain records
- `purchase_order_sample_data` - 2 sample PO records
- `plain_schema` - StructType for plain domain

## Writing New Tests

Follow the AAA pattern:

```python
@pytest.mark.unit
def test_should_do_something_when_condition_met(spark_session, plain_sample_data):
    """Clear description of what the test validates."""
    # Arrange
    df = spark_session.createDataFrame(plain_sample_data, schema=plain_schema)

    # Act
    result = df.filter("amount > 50").collect()

    # Assert
    assert len(result) == 2
```

## Troubleshooting

### "ModuleNotFoundError: No module named 'awsglue'"

**This is the most common issue!**

❌ **Wrong**: Running tests on host machine

```bash
poetry run pytest  # Will fail!
```

✅ **Correct**: Running tests inside Docker container

```bash
./scripts/run-tests-docker.bash all
```

### Container not running

```bash
# Check if container is up
docker ps | grep glue-pyspark-poc

# If not, start it
./scripts/start-containers.bash
```

### First time setup inside container

```bash
# Open shell in container
./scripts/run-tests-docker.bash shell

# Install dependencies (first time only)
poetry install
```

### Tests are slow

```bash
# Use fast mode (skips coverage)
./scripts/run-tests-docker.bash fast
```

### Clean test cache

```bash
./scripts/run-tests-docker.bash clean
```

### Debug specific test

```bash
# Open shell for interactive debugging
./scripts/run-tests-docker.bash shell
cd /app
poetry run pytest tests/plain/test_bronze_job.py -vv -s
```

## Next Steps

1. **Review test output** to understand code coverage gaps
2. **Add integration tests** for LocalStack services (future)
3. **Run tests in CI/CD** pipeline (GitHub Actions example in tests/README.md)
4. **Expand edge cases** as you discover new scenarios

## Additional Resources

- Full documentation: `tests/README.md`
- Test guidelines: `.github/prompts/unit-tester.prompt.md`
- Project architecture: `.github/copilot-instructions.md`
