import os
from pyspark.sql.functions import col, current_timestamp, date_format
from pyspark.sql.types import StringType
from session import get_spark_session

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")

def read_kafka_stream():
    """
    Inicia a leitura do Kafka via PySpark Structured Streaming e aplica
    o schema da Camada Bronze, extraindo metadados e o raw payload.
    """
    spark = get_spark_session("BronzeIngestion")
    
    print(f"Reading from Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    
    # Leitura em formato streaming nativa do Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
        
    # Transformação de dados de byte-array para String e formatação de acordo
    # com o `bronze_schema` do 02-data-modeling.md
    bronze_df = df.select(
        col("key").cast(StringType()).alias("kafka_key"),
        col("value").cast(StringType()).alias("raw_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    ).withColumn(
        "ingestion_timestamp", current_timestamp()
    ).withColumn(
        "ingestion_date", date_format(col("ingestion_timestamp"), "yyyy-MM-dd")
    )
    
    return bronze_df
BRONZE_PATH = os.getenv("S3_BRONZE_PATH", "s3a://lakehouse/bronze/transactions/")

def write_bronze_stream(bronze_df):
    """
    Grava os dados brutos da Camada Bronze no Storage S3 usando o formato Delta.
    Estratégia: Append-only
    Partição: ingestion_date
    """
    print(f"Writing Bronze stream to {BRONZE_PATH}")
    
    # Inicia a escrita contínua (Streaming)
    query = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("ingestion_date") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/bronze_checkpoints") \
        .start(BRONZE_PATH)
        
    return query

if __name__ == "__main__":
    # Teste unitário manual do fluxo de leitura e escrita
    bronze_stream_df = read_kafka_stream()
    print("Bronze stream read logic created successfully.")
    
    # Inicia a escrita (bloqueia o terminal enquanto escuta o stream)
    # query = write_bronze_stream(bronze_stream_df)
    # query.awaitTermination()
    
    bronze_stream_df.printSchema()
