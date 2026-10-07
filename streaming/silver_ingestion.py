import os
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from session import get_spark_session

BRONZE_PATH = os.getenv("S3_BRONZE_PATH", "s3a://lakehouse/bronze/transactions/")

# Schema Enforcement rigoroso para os dados contidos no JSON da Camada Bronze
payload_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("region", StringType(), False),
    StructField("event_time", TimestampType(), False)
])

def process_silver_stream():
    """
    Lê os dados raw da Camada Bronze (Stream) e aplica o Schema Enforcement 
    fazendo o parsing seguro do JSON bruto.
    """
    spark = get_spark_session("SilverIngestion")
    
    print(f"Reading Bronze stream from {BRONZE_PATH}")
    
    # Lendo o Delta Lake Bronze como um Stream
    bronze_stream = spark.readStream \
        .format("delta") \
        .load(BRONZE_PATH)
        
    # Faz o parsing do raw_payload JSON usando o Schema Enforcement estrito
    parsed_df = bronze_stream.withColumn(
        "data", from_json(col("raw_payload"), payload_schema)
    ).select(
        col("data.*"), # Achata o JSON transformando as propriedades em colunas
        col("kafka_offset"), # Rastros de auditoria da mensageria
        col("ingestion_date").alias("bronze_ingestion_date")
    )
    
    return parsed_df

if __name__ == "__main__":
    silver_df = process_silver_stream()
    print("Silver stream schema enforcement configured successfully.")
    silver_df.printSchema()
