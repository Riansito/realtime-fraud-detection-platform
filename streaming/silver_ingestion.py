import os

import structlog
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    hour,
    lit,
    struct,
    to_date,
    to_json,
    when,
)
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

# Strict Schema Enforcement for the JSON data in the Bronze Layer
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

    # Reading the Bronze Delta Lake as a Stream
    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    # Parsing the raw_payload JSON using strict Schema Enforcement
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

    # Structural validation: Adds flag and failure reason for DLQ routing
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

    # Applies 10-minute Watermarking based on event_time
    # Essential for clearing state memory in the next Deduplication step
    watermarked_df = validated_df.withWatermark("event_time", "10 minutes")

    # Semantic Deduplication (Stateful Operation protected by Watermark)
    # Ensures that if the producer or Kafka resends the same message within 10 minutes, it is discarded.
    deduplicated_df = watermarked_df.dropDuplicates(["transaction_id", "event_time"])

    # =========================================================================
    # FRAUD ENGINE (FAST PATH)
    # =========================================================================
    # Consolidated Score Calculation (BACK-023)

    # Extracts the event hour for the Nighttime Heuristic
    risk_df = deduplicated_df.withColumn("event_hour", hour(col("event_time")))

    # High-risk categories
    suspicious_categories = ["crypto", "jewelry", "gambling"]

    # Partial Weights Definition
    base_risk = lit(0.05)
    score_value = when(col("amount") > 5000, 0.40).otherwise(0.0)
    score_time = when(
        (col("event_hour") >= 0) & (col("event_hour") <= 5), 0.30
    ).otherwise(0.0)
    score_category = when(
        col("merchant_category").isin(suspicious_categories), 0.30
    ).otherwise(0.0)

    # Sum of Weights
    from pyspark.sql.functions import least

    total_score = base_risk + score_value + score_time + score_category

    risk_df = (
        risk_df.withColumn(
            "risk_score",
            least(total_score, lit(1.0)),  # Limits the maximum score to 1.0 (100%)
        )
        .withColumn(
            "is_fraud_suspect",
            col("risk_score") >= 0.70,  # Flag triggers if it exceeds 70%
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
    )  # Removes the temporary column

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
        # Spark calls this function every 10 seconds with a static batch (DataFrame)

        # 1. Success Route (Valid Data)
        valid_df = batch_df.filter(col("is_valid") == True).drop(
            "is_valid", "dlq_reason", "raw_payload"
        )  # Clean debug columns

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

            # Creates the event_date column (DATE in Postgres)
            valid_df = valid_df.withColumn("event_date", to_date(col("event_time")))

            # --- ENGINE DE FRAUDE: Regra 3 (BACK-021) - Heurística de Alta Velocidade ---
            # Evaluates brute force attacks by counting the number of transactions da mesma conta neste micro-lote
            from pyspark.sql.functions import count as spark_count
            from pyspark.sql.functions import least
            from pyspark.sql.window import Window

            window_spec = Window.partitionBy("account_id")
            valid_df = valid_df.withColumn(
                "tx_count_batch", spark_count("transaction_id").over(window_spec)
            )

            # Adds high Velocity weight to the Score Consolidado
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
            )  # Clean temporary column

            # Saves to DW (Postgres) via JDBC to feed the Silver layer
            jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'postgres')}?sslmode={os.getenv('POSTGRES_SSLMODE', 'require')}&stringtype=unspecified"

            valid_df.write.format("jdbc").option("url", jdbc_url).option(
                "dbtable", "silver.transactions"
            ).option("user", os.getenv("POSTGRES_USER", "postgres")).option(
                "password", os.getenv("POSTGRES_PASSWORD", "postgres")
            ).option("driver", "org.postgresql.Driver").mode("append").save()

            # --- ENGINE DE FRAUDE: Publicação de Alertas em Tempo Real (BACK-024) ---
            # If fraud is suspected, we don't wait for dbt/Gold Layer.
            # Fire an immediate event back to Kafka para a API atuar e bloquear.
            alerts_df = valid_df.filter(col("is_fraud_suspect") == True)

            if not alerts_df.isEmpty():
                # Kafka requires key and value in String/Binary
                kafka_alerts = alerts_df.select(
                    col("transaction_id").alias(
                        "key"
                    ),  # Uses transaction_id as Partition Key
                    to_json(struct(*alerts_df.columns)).alias(
                        "value"
                    ),  # Entire row formatted as JSON
                )

                logger.info(
                    "publishing_fraud_alerts",
                    count=alerts_df.count(),
                    topic=ALERTS_TOPIC,
                )
                kafka_alerts.write.format("kafka").option(
                    "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
                ).option("topic", ALERTS_TOPIC).save()

        # 2. Failure Route (Dead Letter Queue)
        invalid_df = batch_df.filter(col("is_valid") == False)

        if not invalid_df.isEmpty():
            invalid_df.write.format("delta").mode("append").partitionBy(
                "bronze_ingestion_date"
            ).save(DLQ_PATH)

    # Initializes the stream trigger
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
    # Full Silver Layer Pipeline
    silver_df = process_silver_stream()

    # Start local tests
    query = write_silver_stream(silver_df)
    query.awaitTermination()

    logger.info("silver_stream_pipeline_configured")
