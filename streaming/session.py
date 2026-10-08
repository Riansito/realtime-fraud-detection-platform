import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

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
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    # Configure Spark Session
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        # Default (200) creates 200 S3-backed state stores per batch -> very slow
        .config(
            "spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3_ENDPOINT_URL", "").replace("https://", ""),
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config(
            "spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    return builder.getOrCreate()


if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark Session initialized successfully with versions:")
    print(f"Spark version: {spark.version}")
    spark.stop()
