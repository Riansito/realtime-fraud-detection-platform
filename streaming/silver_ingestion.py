import os
from pyspark.sql.functions import col, from_json, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from session import get_spark_session

BRONZE_PATH = os.getenv("S3_BRONZE_PATH", "s3a://lakehouse/bronze/transactions/")
DLQ_PATH = os.getenv("S3_DLQ_PATH", "s3a://lakehouse/dlq/transactions/")

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
    fazendo o parsing seguro do JSON bruto. Adiciona validação para DLQ.
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
        col("raw_payload"), # Guardado para caso caia na DLQ podermos debugar o JSON original
        col("data.*"), # Achata o JSON transformando as propriedades em colunas
        col("kafka_offset"), # Rastros de auditoria da mensageria
        col("ingestion_date").alias("bronze_ingestion_date")
    )
    
    # Validação estrutural: Adiciona flag e motivo de falha para direcionamento à DLQ
    validated_df = parsed_df.withColumn(
        "is_valid",
        when(col("transaction_id").isNull() | col("amount").isNull() | (col("amount") <= 0), lit(False))
        .otherwise(lit(True))
    ).withColumn(
        "dlq_reason",
        when(col("transaction_id").isNull(), lit("Schema Enforcement Failed: Missing transaction_id ou JSON malformado"))
        .when(col("amount").isNull(), lit("Missing amount"))
        .when(col("amount") <= 0, lit("Invalid amount (negative or zero)"))
        .otherwise(lit(None))
    )
    
    # Aplica Watermarking de 10 minutos baseado no horário do evento (event_time)
    # Fundamental para limpar o estado (State) da memória na próxima etapa de Deduplicação
    watermarked_df = validated_df.withWatermark("event_time", "10 minutes")
    
    # Deduplicação Semântica (Stateful Operation protegida pelo Watermark)
    # Garante que, se o produtor ou o Kafka reenviarem a mesma mensagem dentro de 10 minutos, ela será descartada.
    deduplicated_df = watermarked_df.dropDuplicates(["transaction_id", "event_time"])
    
    # =========================================================================
    # ENGINE DE FRAUDE (FAST PATH)
    # =========================================================================
    # Regra 1 (BACK-019): Heurística de Valor Anômalo (Montantes excepcionalmente altos)
    risk_df = deduplicated_df.withColumn(
        "is_fraud_suspect",
        when(col("amount") > 5000, lit(True))
        .otherwise(lit(False))
    ).withColumn(
        "fraud_reason",
        when(col("amount") > 5000, lit("High Risk: Anomalous high value transaction (amount > 5000)"))
        .otherwise(lit(None))
    ).withColumn(
        "risk_score",
        when(col("amount") > 5000, lit(0.85)) # Atribui 85% de risco base
        .otherwise(lit(0.05)) # Risco comum de 5%
    ).withColumn(
        "processed_at", current_timestamp()
    )
    
    return risk_df

SILVER_PATH = os.getenv("S3_SILVER_PATH", "s3a://lakehouse/silver/transactions/")
CHECKPOINT_DIR = os.getenv("S3_CHECKPOINT_DIR", "s3a://lakehouse/checkpoints/")

def write_silver_stream(silver_stream_df):
    """
    Usa a estratégia foreachBatch para dividir o micro-lote de Streaming:
    - Dados válidos vão para a Camada Silver (Prontos para análise de Fraude).
    - Dados inválidos vão para a tabela Delta de DLQ.
    """
    
    def route_batch(batch_df, batch_id):
        # O Spark chama essa função a cada 10 segundos com um lote estático (DataFrame)
        
        # 1. Rota de Sucesso (Valid Data)
        valid_df = batch_df.filter(col("is_valid") == True) \
            .drop("is_valid", "dlq_reason", "raw_payload") # Limpamos colunas de debug
            
        if not valid_df.isEmpty():
            # Cria a partição event_date fisicamente
            from pyspark.sql.functions import date_format
            valid_df = valid_df.withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
            
            valid_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("event_date") \
                .save(SILVER_PATH)
                
        # 2. Rota de Falha (Dead Letter Queue)
        invalid_df = batch_df.filter(col("is_valid") == False)
        
        if not invalid_df.isEmpty():
            invalid_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("bronze_ingestion_date") \
                .save(DLQ_PATH)
                
    # Inicializa o gatilho da stream
    print("Starting Silver & DLQ write streams...")
    query = silver_stream_df.writeStream \
        .foreachBatch(route_batch) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver") \
        .start()
        
    return query

if __name__ == "__main__":
    # Pipeline Completo da Camada Silver
    silver_df = process_silver_stream()
    
    # Descomente para testes locais
    # query = write_silver_stream(silver_df)
    # query.awaitTermination()
    
    print("Silver stream Pipeline (Schema Enforcement -> DLQ -> Watermark -> Deduplication -> Write) Configured Successfully!")
