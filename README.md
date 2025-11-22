# Docker Glue PySpark Demo

A local development environment for building and testing data processing pipelines using AWS Glue, Apache Spark, and streaming data sources.

## What This Does

This project helps you develop and test cloud data pipelines on your local machine before deploying to AWS. It simulates a complete data processing workflow that:

1. **Ingests streaming data** from a message queue (Kafka)
2. **Processes data in three stages**:
   - **Bronze Layer**: Captures raw data as-is
   - **Silver Layer**: Cleans and enriches the data
   - **Gold Layer**: Creates business-ready aggregated reports
3. **Stores results** in cloud-compatible storage systems

## Key Features

- 🐳 **Fully containerized** - Everything runs in Docker, no complex local setup
- ☁️ **Cloud-native** - Uses LocalStack to simulate AWS services (S3, DynamoDB, CloudWatch)
- 📊 **Data lakehouse architecture** - Implements Apache Iceberg for modern data management
- 🔄 **Real-time processing** - Handles streaming data from Kafka topics
- 📓 **Interactive exploration** - Includes JupyterLab for data analysis

## Use Cases

This setup is ideal for:

- Learning data engineering concepts (medallion architecture, data lakes)
- Prototyping data pipelines before cloud deployment
- Testing data transformations locally
- Developing AWS Glue jobs without AWS costs

## Quick Start

1. Start the environment:

   ```bash
   ./scripts/start-containers.bash
   ```

2. Set up AWS resources:

   ```bash
   cd terraform && terraform apply -auto-approve
   ```

3. Run a sample pipeline:
   ```bash
   docker exec -it glue-pyspark-poc python /app/scripts/generate_plain_events.py 10
   docker exec -it glue-pyspark-poc poetry run python /app/plain/bronze_job.py
   ```

## What's Inside

- **Two example pipelines**: Simple demo and realistic purchase order processing
- **AWS Glue 5.0 runtime**: Same environment as production AWS
- **Apache Iceberg**: Modern table format for data lakes
- **Kafka**: For streaming data ingestion
- **LocalStack**: Local AWS cloud simulation
- **JupyterLab**: For interactive data exploration

## Architecture

The project follows the **medallion architecture** pattern:

- **Bronze** → Raw data ingestion (no transformations)
- **Silver** → Cleaned, validated, enriched data
- **Gold** → Business-ready aggregated metrics

Data flows from Kafka → S3 (Bronze) → Iceberg Tables (Silver) → DynamoDB (Gold)
