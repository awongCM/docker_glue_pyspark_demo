# Test Suite Documentation

## Overview

Comprehensive unit test suite for the Docker Glue PySpark Demo project covering both **plain** and **purchase_order** medallion architecture pipelines (Bronze → Silver → Gold layers).

## Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures and test configuration
├── plain/
│   ├── __init__.py
│   ├── test_bronze_job.py   # Plain Bronze layer tests (Kafka → S3)
│   ├── test_silver_job.py   # Plain Silver layer tests (S3 → Iceberg)
│   └── test_gold_job.py     # Plain Gold layer tests (Iceberg → DynamoDB)
└── purchase_order/
    ├── __init__.py
    ├── test_bronze_job.py   # PO Bronze layer tests (Kafka → S3)
    ├── test_silver_job.py   # PO Silver layer tests (S3 → Iceberg)
    └── test_gold_job.py     # PO Gold layer tests (Iceberg → DynamoDB)
```

## Test Philosophy

All tests follow the **AAA (Arrange-Act-Assert)** pattern as defined in `.github/prompts/unit-tester.prompt.md`:

1. **Arrange**: Set up test data, fixtures, and preconditions
2. **Act**: Execute the function/method under test
3. **Assert**: Verify results and side effects

### Naming Convention

Tests are named using the pattern: `test_should_ExpectedBehavior_when_StateUnderTest`

Example: `test_should_filter_records_above_threshold_when_cleaning_data`

## Running Tests

⚠️ **IMPORTANT**: Tests must run inside the `glue-pyspark-poc` Docker container because they require the `awsglue` module.

### Prerequisites

1. **Start Docker containers:**

   ```bash
   ./scripts/start-containers.bash
   ```

2. **Verify container is running:**
   ```bash
   docker ps | grep glue-pyspark-poc
   ```

### Run Tests Inside Container

#### Using the test runner script (Recommended):

```bash
# Run all tests with coverage
./scripts/run-tests-docker.bash all

# Run tests with verbose output
./scripts/run-tests-docker.bash verbose

# Open interactive shell for manual testing
./scripts/run-tests-docker.bash shell
```

#### Manual execution:

```bash
# Enter container
docker exec -it glue-pyspark-poc bash

# Inside container
cd /app
poetry install  # First time only
poetry run pytest -v
exit
```

### Run Specific Test Categories

Using the Docker test runner:

```bash
# Run only plain pipeline tests
./scripts/run-tests-docker.bash plain

# Run only purchase_order pipeline tests
./scripts/run-tests-docker.bash po

# Run specific layer
./scripts/run-tests-docker.bash bronze
./scripts/run-tests-docker.bash silver
./scripts/run-tests-docker.bash gold
```

Manual execution inside container:

```bash
# First open shell
./scripts/run-tests-docker.bash shell

# Then run specific tests
poetry run pytest tests/plain/
poetry run pytest tests/purchase_order/
poetry run pytest tests/plain/test_bronze_job.py
poetry run pytest tests/plain/test_silver_job.py::TestSilverJobDataTransformation
poetry run pytest tests/plain/test_gold_job.py::TestGoldJobDynamoDBOperations::test_should_format_item_correctly_when_writing_to_dynamodb
```

### Run Tests by Markers

### Generate Coverage Reports

```bash
# Generate coverage report inside container
./scripts/run-tests-docker.bash coverage

# Open HTML coverage report (on your host machine)
open htmlcov/index.html

# Manual approach
docker exec -it glue-pyspark-poc bash -c "cd /app && poetry run pytest --cov=plain --cov=purchase_order --cov-report=html --cov-report=term"
```

### Generate Coverage Reports

```bash
# Generate terminal coverage report
poetry run pytest --cov=plain --cov=purchase_order --cov-report=term-missing

# Generate HTML coverage report
poetry run pytest --cov=plain --cov=purchase_order --cov-report=html

# Open HTML coverage report (macOS)
open htmlcov/index.html
```

## Test Coverage

### Plain Pipeline Tests

#### `test_bronze_job.py`

- CloudWatch logging functionality
- Kafka schema validation (id, name, amount)
- Configuration constants (topics, S3 paths, endpoints)
- Data processing logic for non-empty and empty batches

#### `test_silver_job.py`

- CloudWatch logging
- S3 and Iceberg catalog configuration
- Database and table creation logic
- Data transformation (filtering amount < 100)
- Column preservation and boundary value handling

#### `test_gold_job.py`

- CloudWatch logging
- Iceberg table reading configuration
- DynamoDB item formatting (type annotations: N, S)
- Batch writing to DynamoDB
- Edge cases: empty datasets, special characters, zero values, large numbers

### Purchase Order Pipeline Tests

#### `test_bronze_job.py`

- CloudWatch logging functionality
- Complex nested schema validation (contact_info, items array)
- Field type validation (DoubleType for monetary, DateType for dates)
- Nested structure handling and array processing
- Monetary precision preservation

#### `test_silver_job.py`

- CloudWatch logging
- Iceberg catalog configuration (hadoop_catalog)
- Flattened schema validation (12 columns)
- Items array explosion (denormalization)
- Contact_info flattening
- Weight quantity filtering (> 5000)
- Boundary value testing

#### `test_gold_job.py`

- CloudWatch logging
- Aggregation by customer email
- Sum and average calculations
- Decimal conversion for monetary precision
- DynamoDB item formatting with aggregated data
- Edge cases: empty datasets, special characters in email, zero totals

## Fixtures

### Shared Fixtures (`conftest.py`)

- **`spark_session`**: Local SparkSession for testing (session-scoped)
- **`mock_boto3_logs_client`**: Mocked CloudWatch Logs client
- **`mock_boto3_s3_client`**: Mocked S3 client (using moto)
- **`mock_boto3_dynamodb_client`**: Mocked DynamoDB client (using moto)
- **`plain_sample_data`**: Sample plain domain events (4 records)
- **`purchase_order_sample_data`**: Sample purchase order events (2 orders, 3 line items)
- **`plain_schema`**: StructType schema for plain domain

## Mocking Strategy

### AWS Services

Tests use `moto` library for mocking AWS services (S3, DynamoDB, CloudWatch Logs) to avoid:

- External dependencies during testing
- Network calls to LocalStack or AWS
- Complex test environment setup

### PySpark/Glue

Tests isolate transformation logic from streaming and I/O:

- Use local SparkSession for DataFrame operations
- Mock GlueContext and DynamicFrame where necessary
- Focus on testing business logic, not infrastructure

## Test Isolation

- **Unit tests** are marked with `@pytest.mark.unit` and don't require external services
- **Integration tests** (future) would be marked with `@pytest.mark.integration` and test against LocalStack
- Each test is independent and can run in any order

## Best Practices

1. **One behavior per test**: Each test validates a single aspect
2. **Meaningful test data**: Sample data represents realistic scenarios
3. **Docstrings**: Every test has a clear docstring explaining its purpose
4. **AAA comments**: Tests include `# Arrange`, `# Act`, `# Assert` comments
5. **Edge cases covered**: Boundary values, empty datasets, special characters
6. **Negative tests**: Tests for expected failures and filtering logic

## Common Test Patterns

### Testing Data Transformations

```python
def test_should_filter_records_above_threshold_when_cleaning_data(
    self, spark_session, plain_sample_data, plain_schema
):
    """Test description following naming convention."""
    # Arrange
    df = spark_session.createDataFrame(plain_sample_data, schema=plain_schema)
    threshold = 100

    # Act
    cleaned_df = df.filter(f"amount < {threshold}")
    result = cleaned_df.collect()

    # Assert
    assert len(result) == 3
    assert all(row['amount'] < threshold for row in result)
```

### Testing CloudWatch Logging

```python
def test_should_log_message_to_cloudwatch_when_valid_message_provided(
    self, mock_boto3_logs_client
):
    """Test CloudWatch logging with mocked client."""
    # Arrange
    from plain.bronze_job import log_logging_events
    test_message = "Test log"

    # Act
    log_logging_events(test_message, mock_boto3_logs_client)

    # Assert
    mock_boto3_logs_client.put_log_events.assert_called_once()
    call_args = mock_boto3_logs_client.put_log_events.call_args
    assert call_args.kwargs['logEvents'][0]['message'] == test_message
```

### Testing DynamoDB Formatting

```python
def test_should_format_item_correctly_when_writing_to_dynamodb(self):
    """Test DynamoDB item structure."""
    # Arrange
    record = {"id": 1, "name": "Alice", "amount": 50}

    # Act
    item = {
        'id': {'N': str(record['id'])},
        'name': {'S': record['name']},
        'amount': {'N': str(record['amount'])}
    }

    # Assert
    assert item['id'] == {'N': '1'}
    assert item['name'] == {'S': 'Alice'}
```

## Continuous Integration

To integrate with CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: poetry run pytest --cov --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.xml
```

## Troubleshooting

### "ModuleNotFoundError: No module named 'awsglue'"

**Problem**: Tests fail with import errors when run locally.

**Solution**: Tests must run inside the Docker container where AWS Glue libraries are installed.

```bash
# Wrong - running on host machine
poetry run pytest

# Correct - running inside Docker container
./scripts/run-tests-docker.bash all
```

### Container Not Running

**Problem**: Test runner reports container is not running.

**Solution**: Start the containers first.

```bash
./scripts/start-containers.bash
docker ps | grep glue-pyspark-poc
```

### Poetry Not Found Inside Container

**Problem**: `poetry: command not found` when running tests.

**Solution**: Rebuild the container to ensure Poetry is installed.

```bash
docker-compose build glue-pyspark
docker-compose up -d glue-pyspark
```

### Tests Dependencies Not Installed

**Problem**: Tests fail with missing pytest or other dependencies.

**Solution**: Install dependencies inside the container (first time setup).

```bash
docker exec -it glue-pyspark-poc bash
cd /app
poetry install
exit
```

### PySpark Session Memory Issues

**Problem**: SparkSession initialization fails with memory errors.

**Solution**: Reduce parallelism in `conftest.py` or increase Docker memory allocation.

```python
# In conftest.py
.config("spark.sql.shuffle.partitions", "1")
.config("spark.default.parallelism", "1")
```

### Coverage Files Permission Issues

**Problem**: Cannot write coverage files.

**Solution**: Clean artifacts and run again.

```bash
./scripts/run-tests-docker.bash clean
./scripts/run-tests-docker.bash all
```

### Tests Running Too Slow

**Problem**: Tests take too long with coverage enabled.

**Solution**: Use fast mode to skip coverage collection.

```bash
./scripts/run-tests-docker.bash fast
```

## Future Enhancements

- [ ] Integration tests against LocalStack services
- [ ] Performance benchmarks for transformation logic
- [ ] Property-based testing with Hypothesis
- [ ] Mutation testing with mutmut
- [ ] Contract tests for schema validation
- [ ] End-to-end pipeline tests

## Contributing

When adding new tests:

1. Follow the AAA pattern
2. Use the naming convention: `test_should_X_when_Y`
3. Add docstrings explaining the test purpose
4. Include edge cases and negative tests
5. Use appropriate markers (`@pytest.mark.unit`, etc.)
6. Update this README if adding new test categories
