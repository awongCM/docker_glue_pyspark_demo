"""
Integration tests for purchase_order/bronze_job.py main function.
These tests execute the actual main() function with mocked AWS services.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch


@pytest.mark.integration
class TestPurchaseOrderBronzeJobIntegration:
    """Integration tests for Purchase Order Bronze layer main function execution."""
    
    @patch('purchase_order.bronze_job.boto3.client')
    @patch('purchase_order.bronze_job.SparkSession')
    @patch('purchase_order.bronze_job.GlueContext')
    def test_should_execute_main_function_when_called(
        self, mock_glue_context, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() function executes without errors with mocked dependencies.
        
        Tests complete execution flow for purchase order pipeline.
        """
        # Arrange - Mock CloudWatch logs client
        mock_logs_client = MagicMock()
        mock_boto3_client.return_value = mock_logs_client
        
        # Mock Spark session
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = ["kafka.jar"]
        mock_spark.sparkContext.getConf().get.return_value = "/jars"
        
        # Mock GlueContext
        mock_glue_instance = MagicMock()
        mock_glue_context.return_value = mock_glue_instance
        mock_glue_instance.get_logger.return_value = MagicMock()
        
        # Mock streaming operations
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
        from purchase_order.bronze_job import main
        
        try:
            main()
        except KeyboardInterrupt:
            pass
        
        # Assert
        mock_boto3_client.assert_called_once()
        assert mock_logs_client.put_log_events.call_count >= 4
        mock_glue_context.assert_called_once()
        mock_spark.readStream.format.assert_called_with("kafka")
    
    @patch('purchase_order.bronze_job.boto3.client')
    @patch('purchase_order.bronze_job.SparkSession')
    @patch('purchase_order.bronze_job.GlueContext')
    def test_should_configure_kafka_subscription_when_reading_stream(
        self, mock_glue_context, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that Kafka stream is configured with correct topic and servers.
        
        Tests Kafka bootstrap servers and topic subscription.
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
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        mock_glue_instance = MagicMock()
        mock_glue_context.return_value = mock_glue_instance
        
        mock_query = MagicMock()
        mock_query.awaitTermination = MagicMock(side_effect=KeyboardInterrupt)
        
        mock_stream_reader = MagicMock()
        mock_spark.readStream = mock_stream_reader
        mock_stream_reader.format.return_value = mock_stream_reader
        mock_stream_reader.option.return_value = mock_stream_reader
        mock_stream_reader.load.return_value = MagicMock()
        
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
        from purchase_order.bronze_job import main
        
        try:
            main()
        except KeyboardInterrupt:
            pass
        
        # Assert - Check Kafka configuration
        option_calls = [str(call) for call in mock_stream_reader.option.call_args_list]
        assert any("kafka.bootstrap.servers" in call for call in option_calls)
        assert any("purchase-order" in call for call in option_calls)
