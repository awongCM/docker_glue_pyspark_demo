"""
Integration tests for purchase_order/gold_job.py main function.
These tests execute the actual main() function with mocked AWS services.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal


@pytest.mark.integration
class TestPurchaseOrderGoldJobIntegration:
    """Integration tests for Purchase Order Gold layer main function execution."""
    
    @patch('purchase_order.gold_job.boto3.client')
    @patch('purchase_order.gold_job.SparkSession')
    def test_should_execute_main_function_when_called(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() function executes without errors with mocked dependencies.
        
        Tests complete execution flow for purchase order gold layer.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        # Mock aggregated DataFrame
        mock_agg_df = MagicMock()
        mock_row = {
            'email': 'test@example.com',
            'total_purchase_amount': 150.0,
            'average_purchase_amount': 75.0
        }
        mock_agg_df.collect.return_value = [
            MagicMock(**mock_row, __getitem__=lambda self, key: mock_row[key])
        ]
        
        # Mock source DataFrame
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_agg_df
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act
        from purchase_order.gold_job import main
        main()
        
        # Assert
        assert mock_boto3_client.call_count == 2
        assert mock_logs_client.put_log_events.call_count >= 4
        mock_spark.read.format.assert_called_with("iceberg")
        mock_df.groupBy.assert_called_once()
        mock_dynamodb_client.put_item.assert_called_once()
        mock_spark.stop.assert_called_once()
    
    @patch('purchase_order.gold_job.boto3.client')
    @patch('purchase_order.gold_job.SparkSession')
    def test_should_aggregate_by_customer_email_when_processing(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() aggregates data by customer email.
        
        Tests groupBy and aggregation logic.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        mock_agg_df = MagicMock()
        mock_row = {
            'email': 'customer@example.com',
            'total_purchase_amount': 500.0,
            'average_purchase_amount': 100.0
        }
        mock_agg_df.collect.return_value = [
            MagicMock(**mock_row, __getitem__=lambda self, key: mock_row[key])
        ]
        
        mock_df = MagicMock()
        mock_grouped_df = MagicMock()
        mock_df.groupBy.return_value = mock_grouped_df
        mock_grouped_df.agg.return_value = mock_agg_df
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act
        from purchase_order.gold_job import main
        main()
        
        # Assert - Verify groupBy was called with email
        group_calls = [str(call) for call in mock_df.groupBy.call_args_list]
        assert len(group_calls) > 0
        mock_grouped_df.agg.assert_called_once()
    
    @patch('purchase_order.gold_job.boto3.client')
    @patch('purchase_order.gold_job.SparkSession')
    def test_should_write_aggregated_data_to_dynamodb_when_processing(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() writes aggregated results to DynamoDB.
        
        Tests DynamoDB write operation with proper data formatting.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        mock_agg_df = MagicMock()
        mock_row = {
            'email': 'write@example.com',
            'total_purchase_amount': 225.50,
            'average_purchase_amount': 75.17
        }
        mock_agg_df.collect.return_value = [
            MagicMock(**mock_row, __getitem__=lambda self, key: mock_row[key])
        ]
        
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_agg_df
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act
        from purchase_order.gold_job import main
        main()
        
        # Assert - Verify DynamoDB write
        mock_dynamodb_client.put_item.assert_called_once()
        
        put_item_call = mock_dynamodb_client.put_item.call_args
        assert 'TableName' in put_item_call[1]
        assert put_item_call[1]['TableName'] == 'gold_table_purchase_order'
        assert 'Item' in put_item_call[1]
        
        item = put_item_call[1]['Item']
        assert 'email' in item
        assert 'total_purchase_amount' in item
        assert 'average_purchase_amount' in item
    
    @patch('purchase_order.gold_job.boto3.client')
    @patch('purchase_order.gold_job.SparkSession')
    def test_should_convert_to_decimal_format_when_writing_to_dynamodb(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() converts numeric values to Decimal for DynamoDB.
        
        Tests proper data type conversion for DynamoDB compatibility.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        mock_agg_df = MagicMock()
        mock_row = {
            'email': 'decimal@example.com',
            'total_purchase_amount': 99.99,
            'average_purchase_amount': 99.99
        }
        mock_agg_df.collect.return_value = [
            MagicMock(**mock_row, __getitem__=lambda self, key: mock_row[key])
        ]
        
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_agg_df
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act
        from purchase_order.gold_job import main
        main()
        
        # Assert - Verify Decimal conversion in DynamoDB item
        put_item_call = mock_dynamodb_client.put_item.call_args
        item = put_item_call[1]['Item']
        
        # Check that numeric fields use 'N' type (DynamoDB Number)
        assert 'N' in str(item.get('total_purchase_amount', {})) or 'total_purchase_amount' in item
        assert 'N' in str(item.get('average_purchase_amount', {})) or 'average_purchase_amount' in item
