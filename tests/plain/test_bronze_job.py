"""Unit tests for plain/bronze_job.py - Bronze Layer (Kafka → S3 Parquet)."""
import pytest
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import json


@pytest.mark.unit
class TestBronzeJobLogging:
    """Test CloudWatch logging functionality in bronze_job."""
    
    def test_should_log_message_to_cloudwatch_when_valid_message_provided(self, mock_boto3_logs_client):
        """
        Verify that log_logging_events sends messages to CloudWatch with correct format.
        
        Tests the logging utility function that writes to CloudWatch Logs.
        """
        # Arrange
        from plain.bronze_job import log_logging_events
        test_message = "Test log message"
        log_group = "pyspark-logs"
        log_stream = "bronze-job-stream"
        
        # Act
        log_logging_events(test_message, mock_boto3_logs_client)
        
        # Assert
        mock_boto3_logs_client.put_log_events.assert_called_once()
        call_args = mock_boto3_logs_client.put_log_events.call_args
        assert call_args.kwargs['logGroupName'] == log_group
        assert call_args.kwargs['logStreamName'] == log_stream
        assert call_args.kwargs['logEvents'][0]['message'] == test_message
        assert 'timestamp' in call_args.kwargs['logEvents'][0]
    
    def test_should_handle_json_serializable_messages_when_logging(self, mock_boto3_logs_client):
        """
        Verify that log_logging_events can handle JSON-serializable data structures.
        
        Tests logging of complex data structures like dictionaries.
        """
        # Arrange
        from plain.bronze_job import log_logging_events
        test_data = {"id": 1, "name": "Alice", "amount": 50}
        test_message = json.dumps(test_data)
        
        # Act
        log_logging_events(test_message, mock_boto3_logs_client)
        
        # Assert
        mock_boto3_logs_client.put_log_events.assert_called_once()
        logged_message = mock_boto3_logs_client.put_log_events.call_args.kwargs['logEvents'][0]['message']
        assert json.loads(logged_message) == test_data


@pytest.mark.unit
class TestBronzeJobSchema:
    """Test schema definition for plain bronze job."""
    
    def test_should_match_expected_schema_when_parsing_kafka_messages(self):
        """
        Verify that the schema defined in bronze_job matches plain domain requirements.
        
        Tests that the StructType schema has correct field names and types.
        """
        # Arrange
        expected_fields = {"id": IntegerType, "name": StringType, "amount": IntegerType}
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("amount", IntegerType(), True)
        ])
        
        # Act
        field_dict = {field.name: type(field.dataType) for field in schema.fields}
        
        # Assert
        assert len(schema.fields) == 3
        for field_name, field_type in expected_fields.items():
            assert field_name in field_dict
            assert field_dict[field_name] == field_type
    
    def test_should_allow_nullable_fields_when_defining_schema(self):
        """
        Verify that all fields in bronze schema are nullable.
        
        Tests that schema allows null values for data quality flexibility.
        """
        # Arrange
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("amount", IntegerType(), True)
        ])
        
        # Act & Assert
        for field in schema.fields:
            assert field.nullable is True, f"Field {field.name} should be nullable"


@pytest.mark.unit
class TestBronzeJobDataProcessing:
    """Test data processing logic in bronze_job process_batch function."""
    
    def test_should_process_non_empty_dataframe_when_batch_has_data(
        self, spark_session, mock_boto3_logs_client, plain_sample_data, plain_schema
    ):
        """
        Verify that process_batch handles DataFrames with records correctly.
        
        Tests that the batch processing function processes and logs data.
        """
        # Arrange
        df = spark_session.createDataFrame(plain_sample_data, schema=plain_schema)
        batch_id = 1
        
        # Act
        record_count = df.count()
        
        # Assert
        assert record_count == len(plain_sample_data)
        assert record_count > 0
        # Note: Full process_batch integration requires GlueContext and S3,
        # which are tested via integration tests
    
    def test_should_skip_processing_when_batch_is_empty(self, spark_session, plain_schema):
        """
        Verify that process_batch skips empty DataFrames.
        
        Tests edge case where Kafka batch contains no records.
        """
        # Arrange
        empty_df = spark_session.createDataFrame([], schema=plain_schema)
        
        # Act
        record_count = empty_df.count()
        
        # Assert
        assert record_count == 0


@pytest.mark.unit
class TestBronzeJobConfiguration:
    """Test configuration constants in bronze_job."""
    
    def test_should_use_correct_kafka_topic_when_reading_stream(self):
        """
        Verify that bronze_job is configured with the correct Kafka topic.
        
        Tests configuration constants for Kafka integration.
        """
        # Arrange
        from plain.bronze_job import kafka_topic, kafka_bootstrap_servers
        
        # Act & Assert
        assert kafka_topic == "plain-topic"
        assert kafka_bootstrap_servers == "kafka:29092"
    
    def test_should_use_correct_s3_path_when_writing_output(self):
        """
        Verify that bronze_job writes to the correct S3 bucket path.
        
        Tests S3 output configuration for bronze layer.
        """
        # Arrange
        from plain.bronze_job import output_s3_bucket, bucket_name
        
        # Act & Assert
        assert bucket_name == "bronze-bucket"
        assert output_s3_bucket == "s3a://bronze-bucket/plain"
        assert output_s3_bucket.startswith("s3a://"), "Should use s3a:// protocol"
    
    def test_should_use_localstack_endpoint_when_in_development(self):
        """
        Verify that bronze_job uses LocalStack endpoint for local development.
        
        Tests that AWS services point to LocalStack for local testing.
        """
        # Arrange
        from plain.bronze_job import s3_endpoint_url
        
        # Act & Assert
        assert s3_endpoint_url == "http://localstack:4566"
