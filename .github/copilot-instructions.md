# Copilot Instructions

## Project Overview

Local AWS Glue development environment simulating PySpark/Glue jobs with Kafka, Iceberg, S3, and DynamoDB using Docker and LocalStack. Implements a **medallion architecture** (Bronze → Silver → Gold) for data processing pipelines.

## Architecture & Data Flow

### Three-Layer Pipeline Pattern

1. **Bronze Layer** (Raw Ingestion): Kafka → S3 Parquet

   - Uses `awsglue.context.GlueContext` and `DynamicFrame` API
   - Reads from Kafka topics, writes to `s3a://bronze-bucket/`
   - Files: `plain/bronze_job.py`, `purchase_order/bronze_job.py`

2. **Silver Layer** (Cleaned/Enriched): S3 Parquet → Iceberg Tables

   - Uses PySpark `SparkSession` and `DataFrame` API (NOT GlueContext)
   - Reads from bronze S3, writes to Iceberg catalog with Hive metastore
   - Files: `plain/silver_job.py`, `purchase_order/silver_job.py`

3. **Gold Layer** (Aggregated): Iceberg → DynamoDB
   - Uses PySpark `SparkSession` (NOT GlueContext)
   - Aggregates data and writes to DynamoDB tables
   - Files: `plain/gold_job.py`, `purchase_order/gold_job.py`

### Service Endpoints (LocalStack)

- **Inside containers**: `http://localstack:4566` (S3, DynamoDB, CloudWatch)
- **From host**: `http://localhost:4566`
- **Kafka internal**: `kafka:29092`
- **Kafka external**: `localhost:9092`

## Critical Workflows

### Environment Setup

```bash
# Start all services (Kafka, LocalStack, Spark containers)
./scripts/start-containers.bash

# Provision AWS resources (S3 buckets, DynamoDB tables, CloudWatch logs)
cd terraform && terraform init && terraform apply -auto-approve

# Create Kafka topics
docker exec -it docker_glue_pyspark_demo-kafka-1 \
  kafka-topics --create --topic plain-topic --bootstrap-server kafka:9092
docker exec -it docker_glue_pyspark_demo-kafka-1 \
  kafka-topics --create --topic purchase-order --bootstrap-server kafka:9092
```

### Running Jobs

```bash
# Execute jobs from inside the glue-pyspark container
docker exec -it glue-pyspark-poc bash

# Run Bronze job (use poetry run for dependency isolation)
poetry run python /app/plain/bronze_job.py

# Run Silver job
poetry run python /app/plain/silver_job.py

# Run Gold job
poetry run python /app/plain/gold_job.py
```

### Generating Test Data

```bash
# Generate events for plain pipeline
docker exec -it glue-pyspark-poc python /app/scripts/generate_plain_events.py 10

# Generate purchase order events
docker exec -it glue-pyspark-poc python /app/scripts/generate_po_events.py 5
```

### Debugging & Inspection

```bash
# Check CloudWatch logs (jobs write to LocalStack CloudWatch)
aws --endpoint-url=http://localhost:4566 logs tail /aws/glue/jobs/pyspark-logs --follow

# Inspect S3 buckets
aws --endpoint-url=http://localhost:4566 s3 ls s3://bronze-bucket/ --recursive

# Consume Kafka messages
docker exec -it docker_glue_pyspark_demo-kafka-1 \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic plain-topic --from-beginning

# Query DynamoDB tables
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name gold_table_plain
```

## Project-Specific Patterns

### GlueContext Usage Rules

- **Bronze jobs ONLY**: Import and use `awsglue.context.GlueContext` with `DynamicFrame`
- **Silver/Gold jobs**: Use standard `pyspark.sql.SparkSession` (importing GlueContext causes errors)

```python
# Bronze pattern
from awsglue.context import GlueContext
glueContext = GlueContext(spark.sparkContext)
dynamic_frame = glueContext.create_dynamic_frame.from_options(...)

# Silver/Gold pattern (NO GlueContext)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Silver Layer").getOrCreate()
df = spark.read.parquet("s3a://bronze-bucket/plain")
```

### Iceberg Configuration Standard

All Silver/Gold jobs require this exact Iceberg catalog configuration:

```python
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.local_catalog.type", "hadoop")
.config("spark.sql.catalog.local_catalog.warehouse", "s3a://iceberg/warehouse")
.config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.endpoint", "http://localstack:4566")
.config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.path.style.access", "true")
```

### S3 URL Format

Always use `s3a://` (NOT `s3://`) in Spark configurations for S3 access.

### JAR Management

- JARs mounted at `/home/glue_user/spark/jars/` in containers
- Bronze jobs require: Kafka + Iceberg + Commons Pool JARs
- Silver/Gold jobs require: Iceberg JARs only
- See `Dockerfile.glue-5-0` for JAR versions

### CloudWatch Logging Pattern

All jobs use this standard logging function:

```python
import boto3, time

logs_client = boto3.client('logs', endpoint_url='http://localstack:4566',
                           region_name='us-east-1',
                           aws_access_key_id='test',
                           aws_secret_access_key='test')

def log_logging_events(message, logs_client):
    log_event = {'timestamp': int(round(time.time() * 1000)), 'message': message}
    logs_client.put_log_events(logGroupName=log_group_name,
                               logStreamName=log_stream_name,
                               logEvents=[log_event])
```

### Dynamic Table Creation

Tables are created conditionally if they don't exist:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local_catalog.{catalog_name}.{table_name} (
        id INT, name STRING, amount INT
    ) USING iceberg
    LOCATION 's3a://iceberg/warehouse/{catalog_name}/{table_name}'
""")
```

## Domain Organization

- **`plain/`**: Simple single-entity pipeline (id, name, amount) - for demonstrations
- **`purchase_order/`**: Complex nested schema (contact_info, items array, totals) - realistic business case
- Both domains follow identical Bronze → Silver → Gold structure

## Key Files Reference

- `docker-compose.yaml`: Service orchestration (5 services: glue-pyspark, jupyterlab, localstack, kafka, zookeeper)
- `Dockerfile.glue-5-0`: AWS Glue 5.0 image with Poetry, JupyterLab, Kafka/Iceberg JARs
- `terraform/main.tf`: S3 buckets, DynamoDB tables, CloudWatch log groups/streams
- `docker_cli.txt`: Kafka topic management commands reference
- `jupyterlab-snippets.md`: Example Spark configurations for notebook exploration

## Common Pitfalls

1. **Network endpoints**: Jobs run inside containers - use `localstack:4566` and `kafka:29092` (NOT `localhost`)
2. **GlueContext imports**: Only in Bronze jobs; causes ClassNotFound errors in Silver/Gold
3. **Kafka topics**: Must be created manually before running Bronze jobs (see `docker_cli.txt`)
4. **Hive metastore conflict**: `hive-site.xml` is removed from Glue image to avoid conflicts
5. **JAR conflicts**: `commons-pool-1.5.4.jar` removed from base image (conflicts with structured streaming)

## Testing Workflow

1. Start containers: `./scripts/start-containers.bash`
2. Apply Terraform: `cd terraform && terraform apply -auto-approve`
3. Create Kafka topics (see docker_cli.txt)
4. Generate test events: `docker exec -it glue-pyspark-poc python /app/scripts/generate_plain_events.py 10`
5. Run Bronze → verify S3: `aws --endpoint-url=http://localhost:4566 s3 ls s3://bronze-bucket/plain/`
6. Run Silver → verify Iceberg: Query via JupyterLab notebook
7. Run Gold → verify DynamoDB: `aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name gold_table_plain`
8. Check logs: `aws --endpoint-url=http://localhost:4566 logs tail pyspark-logs --follow`
