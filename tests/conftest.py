"""Shared pytest fixtures for all tests."""
import pytest
from unittest.mock import MagicMock, Mock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
import boto3
from moto import mock_s3, mock_dynamodb, mock_logs


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession for testing.
    
    This fixture provides a local Spark session for unit tests without
    requiring full cluster dependencies or external services.
    """
    # Arrange
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
    
    # Act & Yield
    yield spark
    
    # Cleanup
    spark.stop()


@pytest.fixture
def mock_boto3_logs_client():
    """
    Mock boto3 CloudWatch Logs client.
    
    Returns a mocked logs client for testing CloudWatch logging functionality
    without making actual AWS API calls.
    """
    # Arrange
    mock_client = MagicMock()
    mock_client.put_log_events.return_value = {
        'nextSequenceToken': 'mock-token',
        'ResponseMetadata': {'HTTPStatusCode': 200}
    }
    
    # Act
    return mock_client


@pytest.fixture
def mock_boto3_s3_client():
    """
    Mock boto3 S3 client using moto.
    
    Provides a mocked S3 client for testing S3 interactions without
    actual AWS infrastructure.
    """
    with mock_s3():
        # Arrange
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:4566',
            region_name='us-east-1',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        
        # Act
        yield s3_client


@pytest.fixture
def mock_boto3_dynamodb_client():
    """
    Mock boto3 DynamoDB client using moto.
    
    Provides a mocked DynamoDB client for testing table interactions
    without actual AWS infrastructure.
    """
    with mock_dynamodb():
        # Arrange
        dynamodb_client = boto3.client(
            'dynamodb',
            endpoint_url='http://localhost:4566',
            region_name='us-east-1',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        
        # Act
        yield dynamodb_client


@pytest.fixture
def plain_sample_data():
    """
    Sample data for plain pipeline testing.
    
    Returns a list of dictionaries representing plain domain events.
    """
    return [
        {"id": 1, "name": "Alice", "amount": 50},
        {"id": 2, "name": "Bob", "amount": 75},
        {"id": 3, "name": "Charlie", "amount": 120},
        {"id": 4, "name": "Diana", "amount": 30}
    ]


@pytest.fixture
def purchase_order_sample_data():
    """
    Sample data for purchase order pipeline testing.
    
    Returns a list of dictionaries representing purchase order events
    with nested structures.
    """
    from datetime import date
    return [
        {
            "contact_info": {
                "name": "John Doe",
                "email": "john@example.com",
                "phone": "555-1234"
            },
            "po_number": "PO-001",
            "items": [
                {"sku": "SKU-001", "description": "Widget A", "weight_quantity": 10000, "price": 25.50},
                {"sku": "SKU-002", "description": "Widget B", "weight_quantity": 8000, "price": 15.75}
            ],
            "subtotal": 41.25,
            "taxes": 3.30,
            "total": 44.55,
            "payment_due_date": date(2025, 12, 31)
        },
        {
            "contact_info": {
                "name": "Jane Smith",
                "email": "jane@example.com",
                "phone": "555-5678"
            },
            "po_number": "PO-002",
            "items": [
                {"sku": "SKU-003", "description": "Gadget C", "weight_quantity": 12000, "price": 99.99}
            ],
            "subtotal": 99.99,
            "taxes": 8.00,
            "total": 107.99,
            "payment_due_date": date(2026, 1, 15)
        }
    ]


@pytest.fixture
def plain_schema():
    """
    Schema for plain domain data.
    
    Returns StructType defining the plain event structure.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
