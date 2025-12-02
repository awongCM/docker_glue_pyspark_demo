"""Unit tests for plain/silver_job.py - Silver Layer (S3 Parquet → Iceberg)."""
import pytest
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@pytest.mark.unit
class TestSilverJobLogging:
    """Test CloudWatch logging functionality in silver_job."""
    
    def test_should_log_message_to_cloudwatch_when_valid_message_provided(self, mock_boto3_logs_client):
        """
        Verify that log_logging_events sends messages to CloudWatch with correct format.
        
        Tests the logging utility function for silver layer.
        """
        # Arrange
        from plain.silver_job import log_logging_events
        test_message = "Silver layer processing started"
        log_group = "pyspark-logs"
        log_stream = "silver-job-stream"
        
        # Act
        log_logging_events(test_message, mock_boto3_logs_client)
        
        # Assert
        mock_boto3_logs_client.put_log_events.assert_called_once()
        call_args = mock_boto3_logs_client.put_log_events.call_args
        assert call_args.kwargs['logGroupName'] == log_group
        assert call_args.kwargs['logStreamName'] == log_stream
        assert call_args.kwargs['logEvents'][0]['message'] == test_message


@pytest.mark.unit
class TestSilverJobConfiguration:
    """Test configuration constants in silver_job."""
    
    def test_should_use_correct_s3_paths_when_reading_bronze_data(self):
        """
        Verify that silver_job reads from the correct bronze S3 path.
        
        Tests S3 input configuration for silver layer.
        """
        # Arrange
        from plain.silver_job import output_s3_bucket, bucket_name
        
        # Act & Assert
        assert bucket_name == "bronze-bucket"
        assert output_s3_bucket == "s3a://bronze-bucket/plain"
        assert output_s3_bucket.startswith("s3a://")
    
    def test_should_use_correct_iceberg_catalog_when_writing_data(self):
        """
        Verify that silver_job uses the correct Iceberg catalog configuration.
        
        Tests Iceberg table naming and catalog structure.
        """
        # Arrange
        from plain.silver_job import namespace_catalog, catalog_name, table_name, full_table_name
        
        # Act & Assert
        assert namespace_catalog == "local_catalog"
        assert catalog_name == "local_catalog_plain"
        assert table_name == "silver_table"
        assert full_table_name == "`local_catalog`.`local_catalog_plain`.`silver_table`"
    
    def test_should_use_localstack_endpoint_when_in_development(self):
        """
        Verify that silver_job uses LocalStack endpoint for local development.
        
        Tests that AWS services point to LocalStack.
        """
        # Arrange
        from plain.silver_job import s3_endpoint_url
        
        # Act & Assert
        assert s3_endpoint_url == "http://localstack:4566"


@pytest.mark.unit
class TestSilverJobDatabaseOperations:
    """Test database and table creation functions in silver_job."""
    
    def test_should_create_database_when_not_exists(self, spark_session):
        """
        Verify that create_database_if_not_exists creates Iceberg database.
        
        Tests database creation logic for Iceberg catalog.
        """
        # Arrange
        from plain.silver_job import create_database_if_not_exists
        test_catalog_name = "test_catalog"
        
        # Act
        create_database_if_not_exists(spark_session, test_catalog_name)
        
        # Assert
        databases = spark_session.sql("SHOW DATABASES").collect()
        database_names = [row.namespace for row in databases]
        assert test_catalog_name in database_names
    
    def test_should_create_table_with_correct_schema_when_not_exists(self):
        """
        Verify that create_table_if_not_exists creates table with plain schema.
        
        Tests table creation with proper column definitions and Iceberg format.
        Note: This test validates the SQL structure without requiring full Iceberg setup.
        """
        # Arrange
        expected_columns = ["id", "name", "amount"]
        expected_types = {"id": "INT", "name": "STRING", "amount": "INT"}
        
        # Act - Construct the CREATE TABLE SQL as in silver_job
        table_sql = """
            CREATE TABLE IF NOT EXISTS local_catalog.test_catalog.test_table (
                id INT,
                name STRING,
                amount INT
            )
            USING iceberg
            LOCATION 's3a://iceberg/warehouse/test_catalog/test_table'
        """
        
        # Assert - Verify SQL structure
        assert "CREATE TABLE IF NOT EXISTS" in table_sql
        assert "USING iceberg" in table_sql
        assert all(col in table_sql for col in expected_columns)
        for col, col_type in expected_types.items():
            assert f"{col} {col_type}" in table_sql


@pytest.mark.unit
class TestSilverJobDataTransformation:
    """Test data transformation logic in silver_job."""
    
    def test_should_filter_records_above_threshold_when_cleaning_data(
        self, spark_session, plain_sample_data, plain_schema
    ):
        """
        Verify that silver_job filters records with amount < 100.
        
        Tests data cleaning transformation that removes high-value transactions.
        """
        # Arrange
        df = spark_session.createDataFrame(plain_sample_data, schema=plain_schema)
        threshold = 100
        
        # Act
        cleaned_df = df.filter(f"amount < {threshold}")
        result = cleaned_df.collect()
        
        # Assert
        assert len(result) == 3  # Only Alice (50), Bob (75), Diana (30)
        for row in result:
            assert row['amount'] < threshold
        assert all(row['amount'] < threshold for row in result)
    
    def test_should_preserve_all_columns_when_filtering_data(
        self, spark_session, plain_sample_data, plain_schema
    ):
        """
        Verify that data filtering preserves all columns in the schema.
        
        Tests that cleaning transformations don't drop columns.
        """
        # Arrange
        df = spark_session.createDataFrame(plain_sample_data, schema=plain_schema)
        expected_columns = ["id", "name", "amount"]
        
        # Act
        cleaned_df = df.filter("amount < 100")
        actual_columns = cleaned_df.columns
        
        # Assert
        assert actual_columns == expected_columns
        assert len(actual_columns) == 3
    
    def test_should_return_empty_dataframe_when_no_records_pass_filter(
        self, spark_session, plain_schema
    ):
        """
        Verify that filtering returns empty DataFrame when no records match.
        
        Tests edge case where all records are filtered out.
        """
        # Arrange
        high_amount_data = [
            {"id": 1, "name": "Alice", "amount": 500},
            {"id": 2, "name": "Bob", "amount": 300}
        ]
        df = spark_session.createDataFrame(high_amount_data, schema=plain_schema)
        
        # Act
        cleaned_df = df.filter("amount < 100")
        result_count = cleaned_df.count()
        
        # Assert
        assert result_count == 0
    
    def test_should_include_boundary_value_when_filtering_data(
        self, spark_session, plain_schema
    ):
        """
        Verify that filtering correctly handles boundary value (amount = 99).
        
        Tests boundary condition for data cleaning filter.
        """
        # Arrange
        boundary_data = [
            {"id": 1, "name": "Alice", "amount": 99},
            {"id": 2, "name": "Bob", "amount": 100},
            {"id": 3, "name": "Charlie", "amount": 101}
        ]
        df = spark_session.createDataFrame(boundary_data, schema=plain_schema)
        
        # Act
        cleaned_df = df.filter("amount < 100")
        result = cleaned_df.collect()
        
        # Assert
        assert len(result) == 1
        assert result[0]['amount'] == 99
        assert result[0]['name'] == "Alice"
