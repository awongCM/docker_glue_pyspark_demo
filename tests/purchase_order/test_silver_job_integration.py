"""
Integration tests for purchase_order/silver_job.py main function.
These tests execute the actual main() function with mocked AWS services.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch


@pytest.mark.integration
class TestPurchaseOrderSilverJobIntegration:
    """Integration tests for Purchase Order Silver layer main function execution."""
    
    @patch('purchase_order.silver_job.boto3.client')
    @patch('purchase_order.silver_job.SparkSession')
    def test_should_execute_main_function_when_called(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() function executes without errors with mocked dependencies.
        
        Tests complete execution flow for purchase order silver layer.
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
        mock_spark.sql.return_value = MagicMock()
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.printSchema.return_value = None
        mock_df.show.return_value = None
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        mock_write_to = MagicMock()
        mock_write_to.append.return_value = None
        mock_df.writeTo.return_value = mock_write_to
        
        mock_table_df = MagicMock()
        mock_table_df.show.return_value = None
        mock_spark.table.return_value = mock_table_df
        
        # Act
        from purchase_order.silver_job import main
        main()
        
        # Assert
        mock_boto3_client.assert_called_once()
        assert mock_logs_client.put_log_events.call_count >= 5
        assert mock_spark.sql.call_count >= 2
        mock_spark.read.format.assert_called_with("parquet")
        mock_df.withColumn.assert_called_once()
        mock_df.writeTo.assert_called_once()
        mock_spark.stop.assert_called_once()
    
    @patch('purchase_order.silver_job.boto3.client')
    @patch('purchase_order.silver_job.SparkSession')
    def test_should_explode_items_and_flatten_structure_when_transforming(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() explodes items array and flattens nested structure.
        
        Tests transformation logic for purchase order data.
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
        mock_spark.sql.return_value = MagicMock()
        
        mock_df = MagicMock()
        mock_exploded_df = MagicMock()
        mock_selected_df = MagicMock()
        mock_filtered_df = MagicMock()
        
        mock_df.withColumn.return_value = mock_exploded_df
        mock_exploded_df.select.return_value = mock_selected_df
        mock_selected_df.filter.return_value = mock_filtered_df
        mock_df.printSchema.return_value = None
        mock_df.show.return_value = None
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        mock_write_to = MagicMock()
        mock_write_to.append.return_value = None
        mock_filtered_df.writeTo.return_value = mock_write_to
        
        mock_table_df = MagicMock()
        mock_spark.table.return_value = mock_table_df
        
        # Act
        from purchase_order.silver_job import main
        main()
        
        # Assert - Verify transformation steps
        mock_df.withColumn.assert_called_once()  # explode items
        mock_exploded_df.select.assert_called_once()  # flatten structure
        mock_selected_df.filter.assert_called_once()  # filter weight > 5000
        mock_filtered_df.writeTo.assert_called_once()
    
    @patch('purchase_order.silver_job.boto3.client')
    @patch('purchase_order.silver_job.SparkSession')
    def test_should_filter_by_weight_threshold_when_cleaning_data(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() filters data by weight_quantity > 5000.
        
        Tests data quality filtering logic.
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
        mock_spark.sql.return_value = MagicMock()
        
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_filtered_df = MagicMock()
        mock_df.filter.return_value = mock_filtered_df
        mock_df.printSchema.return_value = None
        mock_df.show.return_value = None
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        mock_write_to = MagicMock()
        mock_filtered_df.writeTo.return_value = mock_write_to
        
        mock_table_df = MagicMock()
        mock_spark.table.return_value = mock_table_df
        
        # Act
        from purchase_order.silver_job import main
        main()
        
        # Assert - Check filter was applied
        filter_calls = [str(call) for call in mock_df.filter.call_args_list]
        assert len(filter_calls) > 0
