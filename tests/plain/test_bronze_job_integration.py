"""
Integration tests for plain/bronze_job.py main function.
These tests execute the actual main() function with mocked AWS services.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
import json


@pytest.mark.integration
class TestPlainBronzeJobIntegration:
    """Integration tests for Bronze layer main function execution."""
    
    @patch('plain.bronze_job.boto3.client')
    @patch('plain.bronze_job.SparkSession')
    @patch('plain.bronze_job.GlueContext')
    def test_should_execute_main_function_when_called(
        self, mock_glue_context, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() function executes without errors with mocked dependencies.
        
        Tests complete execution flow including Spark initialization, Kafka reading,
        and streaming query setup.
        """
        # Arrange - Mock CloudWatch logs client
        mock_logs_client = MagicMock()
        mock_boto3_client.return_value = mock_logs_client
        
        # Mock Spark session and components
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        # Mock Spark context and configuration
        mock_spark.sparkContext._jsc.sc().listJars.return_value = ["jar1.jar", "jar2.jar"]
        mock_spark.sparkContext.getConf().get.return_value = "/path/to/jars"
        
        # Mock GlueContext
        mock_glue_instance = MagicMock()
        mock_glue_context.return_value = mock_glue_instance
        mock_logger = MagicMock()
        mock_glue_instance.get_logger.return_value = mock_logger
        
        # Mock streaming DataFrame
        mock_stream_reader = MagicMock()
        mock_spark.readStream = mock_stream_reader
        mock_stream_reader.format.return_value = mock_stream_reader
        mock_stream_reader.option.return_value = mock_stream_reader
        mock_stream_reader.load.return_value = MagicMock()
        
        # Mock streaming query
        mock_query = MagicMock()
        mock_query.awaitTermination = MagicMock(side_effect=KeyboardInterrupt)  # Exit after setup
        
        # Mock write stream operations
        mock_df = MagicMock()
        mock_spark.readStream.format().option().option().option().load().selectExpr().select.return_value = mock_df
        mock_write_stream = MagicMock()
        mock_df.writeStream = mock_write_stream
        mock_write_stream.foreachBatch.return_value = mock_write_stream
        mock_write_stream.format.return_value = mock_write_stream
        mock_write_stream.option.return_value = mock_write_stream
        mock_write_stream.trigger.return_value = mock_write_stream
        mock_write_stream.outputMode.return_value = mock_write_stream
        mock_write_stream.start.return_value = mock_query
        
        # Act - Import and execute main
        from plain.bronze_job import main
        
        try:
            main()
        except KeyboardInterrupt:
            pass  # Expected - we trigger this to exit the streaming query
        
        # Assert - Verify key components were called
        mock_boto3_client.assert_called_once()
        assert mock_logs_client.put_log_events.call_count >= 4  # At least 4 log events
        mock_glue_context.assert_called_once_with(mock_spark)
        mock_spark.readStream.format.assert_called_with("kafka")
        
    @patch('plain.bronze_job.boto3.client')
    @patch('plain.bronze_job.SparkSession')
    @patch('plain.bronze_job.GlueContext')
    def test_should_log_spark_initialization_when_main_starts(
        self, mock_glue_context, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() logs Spark initialization details to CloudWatch.
        
        Tests that configuration and JAR information is logged.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_boto3_client.return_value = mock_logs_client
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = ["test.jar"]
        mock_spark.sparkContext.getConf().get.return_value = "/test/jars"
        
        mock_glue_instance = MagicMock()
        mock_glue_context.return_value = mock_glue_instance
        
        mock_query = MagicMock()
        mock_query.awaitTermination = MagicMock(side_effect=KeyboardInterrupt)
        
        mock_df = MagicMock()
        mock_spark.readStream.format().option().option().option().load().selectExpr().select.return_value = mock_df
        mock_write_stream = MagicMock()
        mock_df.writeStream = mock_write_stream
        mock_write_stream.foreachBatch.return_value = mock_write_stream
        mock_write_stream.format.return_value = mock_write_stream
        mock_write_stream.option.return_value = mock_write_stream
        mock_write_stream.trigger.return_value = mock_write_stream
        mock_write_stream.outputMode.return_value = mock_write_stream
        mock_write_stream.start.return_value = mock_query
        
        # Act
        from plain.bronze_job import main
        
        try:
            main()
        except KeyboardInterrupt:
            pass
        
        # Assert - Verify logging calls
        log_calls = mock_logs_client.put_log_events.call_args_list
        assert any("Spark session initialized - jars" in str(call) for call in log_calls)
        assert any("Spark driver - jars loaded in order" in str(call) for call in log_calls)
