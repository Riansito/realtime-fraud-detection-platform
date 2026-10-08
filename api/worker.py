import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
import asyncpg
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("alerts-worker")

# Kafka configs
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_FRAUD_ALERTS = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud_alerts")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP_ALERTS", "fastapi-alerts-operational-group")

# DB configs
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

async def get_db_pool():
    # In local development without SSL, we might need to disable it
    ssl_context = False if DB_SSLMODE == "disable" else True
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        ssl=ssl_context
    )

async def consume_alerts():
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_FRAUD_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    
    await consumer.start()
    
    pool = await get_db_pool()
    
    try:
        logger.info(f"Listening for alerts on topic '{KAFKA_TOPIC_FRAUD_ALERTS}'...")
        async for msg in consumer:
            payload = msg.value
            
            # The event from Spark has is_fraud_suspect, risk_score, fraud_reason, etc.
            # But according to 5.2, we also have risk_level. If missing, we infer it.
            # Some fields like alert_id from Spark are mapped to our DB columns.
            transaction_id = payload.get("transaction_id")
            account_id = payload.get("account_id")
            customer_id = payload.get("customer_id")
            amount = payload.get("amount")
            risk_score = payload.get("risk_score")
            risk_level = payload.get("risk_level", "HIGH" if risk_score < 0.9 else "CRITICAL")
            fraud_reason = payload.get("fraud_reason", "Score exceeded threshold")
            event_time = payload.get("event_time")
            
            logger.info(f"Received alert for transaction {transaction_id} with score {risk_score}")
            
            # Insert into operational.operational_alerts
            query = """
                INSERT INTO operational.operational_alerts (
                    transaction_id, account_id, customer_id, amount, 
                    risk_score, risk_level, fraud_reason, event_time
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8::timestamptz
                ) ON CONFLICT (transaction_id) DO NOTHING;
            """
            
            async with pool.acquire() as conn:
                try:
                    await conn.execute(
                        query,
                        transaction_id,
                        account_id,
                        customer_id,
                        amount,
                        risk_score,
                        risk_level,
                        fraud_reason,
                        event_time
                    )
                    
                    # Commit offset manually after successful processing
                    await consumer.commit()
                    logger.info(f"Successfully processed and committed alert for {transaction_id}")
                except Exception as e:
                    logger.error(f"Failed to process alert for {transaction_id}: {e}")
                    # In a real system, you might send to a DLQ or retry
                    
    finally:
        logger.info("Closing Kafka consumer and DB pool...")
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(consume_alerts())
