import os

import structlog
from pyspark.sql.functions import col, current_timestamp, from_json, hour, lit, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from session import get_spark_session

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger("silver-ingestion")

BRONZE_PATH = os.getenv("S3_BRONZE_PATH", "s3a://lakehouse/bronze/transactions/")
DLQ_PATH = os.getenv("S3_DLQ_PATH", "s3a://lakehouse/dlq/transactions/")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "fraud_alerts")

# Schema Enforcement rigoroso para os dados contidos no JSON da Camada Bronze
payload_schema = StructType(
    [
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
        StructField("event_time", TimestampType(), False),
    ]
)


def process_silver_stream():
    """
    Lê os dados raw da Camada Bronze (Stream) e aplica o Schema Enforcement
    fazendo o parsing seguro do JSON bruto. Adiciona validação para DLQ.
    """
    spark = get_spark_session("SilverIngestion")

    logger.info("reading_bronze_stream", path=BRONZE_PATH)

    # Lendo o Delta Lake Bronze como um Stream
    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    # Faz o parsing do raw_payload JSON usando o Schema Enforcement estrito
    parsed_df = bronze_stream.withColumn(
        "data", from_json(col("raw_payload"), payload_schema)
    ).select(
        col(
            "raw_payload"
        ),  # Guardado para caso caia na DLQ podermos debugar o JSON original
        col("data.*"),  # Achata o JSON transformando as propriedades em colunas
        col("kafka_offset"),  # Rastros de auditoria da mensageria
        col("ingestion_date").alias("bronze_ingestion_date"),
    )

    # Validação estrutural: Adiciona flag e motivo de falha para direcionamento à DLQ
    validated_df = parsed_df.withColumn(
        "is_valid",
        when(
            col("transaction_id").isNull()
            | col("amount").isNull()
            | (col("amount") <= 0),
            lit(False),
        ).otherwise(lit(True)),
    ).withColumn(
        "dlq_reason",
        when(
            col("transaction_id").isNull(),
            lit("Schema Enforcement Failed: Missing transaction_id ou JSON malformado"),
        )
        .when(col("amount").isNull(), lit("Missing amount"))
        .when(col("amount") <= 0, lit("Invalid amount (negative or zero)"))
        .otherwise(lit(None)),
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
    # Cálculo Consolidado de Score (BACK-023)

    # Extrai a hora do evento para a Heurística Noturna
    risk_df = deduplicated_df.withColumn("event_hour", hour(col("event_time")))

    # Categorias de alto risco
    suspicious_categories = ["crypto", "jewelry", "gambling"]

    # Definição de Pesos Parciais (Weights)
    base_risk = lit(0.05)
    score_value = when(col("amount") > 5000, 0.40).otherwise(0.0)
    score_time = when(
        (col("event_hour") >= 0) & (col("event_hour") <= 5), 0.30
    ).otherwise(0.0)
    score_category = when(
        col("merchant_category").isin(suspicious_categories), 0.30
    ).otherwise(0.0)

    # Soma dos Pesos
    from pyspark.sql.functions import least

    total_score = base_risk + score_value + score_time + score_category

    risk_df = (
        risk_df.withColumn(
            "risk_score",
            least(total_score, lit(1.0)),  # Limita o score máximo a 1.0 (100%)
        )
        .withColumn(
            "is_fraud_suspect",
            col("risk_score") >= 0.70,  # Flag acende se passar de 70%
        )
        .withColumn(
            "fraud_reason",
            when(
                col("risk_score") >= 0.70,
                lit("High Risk: Consolidated score exceeded threshold (>= 70%)"),
            ).otherwise(lit(None)),
        )
        .withColumn("processed_at", current_timestamp())
        .drop("event_hour")
    )  # Remove a coluna temporária

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
        valid_df = batch_df.filter(col("is_valid") == True).drop(
            "is_valid", "dlq_reason", "raw_payload"
        )  # Limpamos colunas de debug

        if not valid_df.isEmpty():
            # --- DATA QUALITY ASSERTS (BACK-038) ---
            invalid_amounts = valid_df.filter(
                col("amount").isNull() | (col("amount") <= 0)
            ).count()
            assert invalid_amounts == 0, (
                f"Data Quality Error: Found {invalid_amounts} rows with invalid amount in Silver!"
            )

            valid_currencies = ["USD", "EUR", "BRL", "GBP", "JPY", "CAD", "AUD"]
            invalid_currencies = valid_df.filter(
                ~col("currency").isin(valid_currencies)
            ).count()
            assert invalid_currencies == 0, (
                f"Data Quality Error: Found {invalid_currencies} rows with invalid currency in Silver!"
            )

            # Cria a partição event_date fisicamente
            from pyspark.sql.functions import date_format

            valid_df = valid_df.withColumn(
                "event_date", date_format(col("event_time"), "yyyy-MM-dd")
            )

            # --- ENGINE DE FRAUDE: Regra 3 (BACK-021) - Heurística de Alta Velocidade ---
            # Avalia ataques de força bruta contando o número de transações da mesma conta neste micro-lote
            from pyspark.sql.functions import count as spark_count
            from pyspark.sql.functions import least
            from pyspark.sql.window import Window

            window_spec = Window.partitionBy("account_id")
            valid_df = valid_df.withColumn(
                "tx_count_batch", spark_count("transaction_id").over(window_spec)
            )

            # Adiciona o peso altíssimo de Velocidade ao Score Consolidado
            velocity_score = when(col("tx_count_batch") >= 3, 0.50).otherwise(0.0)

            valid_df = (
                valid_df.withColumn(
                    "risk_score", least(col("risk_score") + velocity_score, lit(1.0))
                )
                .withColumn(
                    "is_fraud_suspect",
                    when(col("risk_score") >= 0.70, lit(True)).otherwise(
                        col("is_fraud_suspect")
                    ),
                )
                .withColumn(
                    "fraud_reason",
                    when(
                        col("risk_score") >= 0.70,
                        lit(
                            "Critical Risk: Velocity or Consolidated Score exceeded threshold"
                        ),
                    ).otherwise(col("fraud_reason")),
                )
                .drop("tx_count_batch")
            )  # Limpa a coluna temporária

            valid_df.write.format("delta").mode("append").partitionBy(
                "event_date"
            ).save(SILVER_PATH)

            # --- ENGINE DE FRAUDE: Publicação de Alertas em Tempo Real (BACK-024) ---
            # Se identificamos fraude (is_fraud_suspect == True), não esperamos o dbt/Gold Layer.
            # Disparamos um evento imediato de volta para o Kafka para a API atuar e bloquear.
            alerts_df = valid_df.filter(col("is_fraud_suspect") == True)

            if not alerts_df.isEmpty():
                # Kafka exige chave (key) e valor (value) em String/Binary
                kafka_alerts = alerts_df.select(
                    col("transaction_id").alias(
                        "key"
                    ),  # Usa transaction_id como Partition Key
                    col("to_json(struct(*))").alias(
                        "value"
                    ),  # Todo o row formatado como JSON
                )

                logger.info(
                    "publishing_fraud_alerts",
                    count=alerts_df.count(),
                    topic=ALERTS_TOPIC,
                )
                kafka_alerts.write.format("kafka").option(
                    "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
                ).option("topic", ALERTS_TOPIC).save()

        # 2. Rota de Falha (Dead Letter Queue)
        invalid_df = batch_df.filter(col("is_valid") == False)

        if not invalid_df.isEmpty():
            invalid_df.write.format("delta").mode("append").partitionBy(
                "bronze_ingestion_date"
            ).save(DLQ_PATH)

    # Inicializa o gatilho da stream
    logger.info("starting_silver_write_streams")
    query = (
        silver_stream_df.writeStream.foreachBatch(route_batch)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver")
        .start()
    )

    return query


if __name__ == "__main__":
    # Pipeline Completo da Camada Silver
    silver_df = process_silver_stream()

    # Iniciar testes locais
    query = write_silver_stream(silver_df)
    query.awaitTermination()

    logger.info("silver_stream_pipeline_configured")
