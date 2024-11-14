import pytest
from pyspark.sql import SparkSession
from src.jobs.customer_etl import CustomerETLJob

class TestCustomerETLJob:
    @pytest.fixture
    def spark(self):
        """Create a Spark session for testing"""
        return SparkSession.builder \
            .appName("CustomerETLJobTest") \
            .master("local[*]") \
            .getOrCreate()
    
    def test_transform(self, spark):
        """Test the transform method of CustomerETLJob"""
        # Create test input data
        test_data = [
            ("1", "John Doe", "john@example.com", 25, 1000),
            ("2", "Jane Smith", "jane@example.com", 17, 500),
            ("3", "Bob Senior", "bob@example.com", 70, 200)
        ]
        
        columns = ["customer_id", "name", "email", "age", "total_purchases"]
        input_df = spark.createDataFrame(test_data, columns)
        
        # Create ETL job instance
        etl_job = CustomerETLJob(spark, "", "")
        
        # Apply transformation
        transformed_df = etl_job.transform(input_df)
        
        # Check transformation results
        result = transformed_df.collect()
        
        # Assertions
        assert len(result) == 3, "Transformed DataFrame should have same number of rows"
        
        # Check age group classifications
        assert result[0]["age_group"] == "Adult", "25-year-old should be classified as Adult"
        assert result[1]["age_group"] == "Minor", "17-year-old should be classified as Minor"
        assert result[2]["age_group"] == "Senior", "70-year-old should be classified as Senior"
        
        # Verify column selection
        expected_columns = ["customer_id", "name", "email", "age_group", "total_purchases"]
        assert transformed_df.columns == expected_columns, "Transformed DataFrame should have correct columns"
    
    def test_etl_job_initialization(self, spark):
        """Test ETL job initialization"""
        input_path = "/test/input/path"
        output_path = "/test/output/path"
        
        etl_job = CustomerETLJob(spark, input_path, output_path)
        
        assert etl_job.spark == spark, "Spark session should be correctly set"
        assert etl_job.input_path == input_path, "Input path should be correctly set"
        assert etl_job.output_path == output_path, "Output path should be correctly set"
