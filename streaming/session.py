import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")

def get_spark_session(app_name="RealTimeFraudDetection"):
    """
    Creates and configures a PySpark session with Delta Lake, Kafka, and AWS S3 extensions.
    """
    # Define required Maven coordinates
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    # Configure Spark Session
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    return builder.getOrCreate()

if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark Session initialized successfully with versions:")
    print(f"Spark version: {spark.version}")
    spark.stop()
