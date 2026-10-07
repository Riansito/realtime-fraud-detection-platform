import json
import time
import uuid
import random
import os
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
# This will try to find .env file from where it is executed, or parent directories
load_dotenv(dotenv_path="../.env") # Adjust if running from root

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")

fake = Faker()

REGIONS = ["BR-SP", "BR-RJ", "US-NY", "US-CA", "EU-UK", "EU-FR"]
MERCHANT_CATEGORIES = ["RETAIL", "FOOD", "ELECTRONICS", "TRAVEL", "ENTERTAINMENT", "HEALTH", "SERVICES"]
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PIX", "PAYPAL", "CRYPTO"]
CURRENCIES = ["BRL", "USD", "EUR", "GBP"]

def generate_transaction():
    """Generates a procedural transaction event."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC-{random.randint(1000, 9999)}",
        "customer_id": f"CUST-{random.randint(100, 999)}",
        "merchant_id": f"MERCH-{random.randint(1000, 9999)}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": str(uuid.uuid4()),
        "ip_address": fake.ipv4(),
        "region": random.choice(REGIONS),
        "event_time": datetime.utcnow().isoformat() + "Z"
    }

def delivery_report(err, msg):
    """Callback triggered on successful/failed delivery."""
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def run_producer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'python-transaction-producer'
    }
    
    print(f"Connecting to Kafka on {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = Producer(conf)
    
    print(f"Producing transactions to topic '{KAFKA_TOPIC}'...")
    try:
        while True:
            transaction = generate_transaction()
            # Send message to Kafka, partitioning by 'region'
            producer.produce(
                KAFKA_TOPIC, 
                key=transaction["region"], 
                value=json.dumps(transaction), 
                callback=delivery_report
            )
            producer.poll(0)
            
            # Procedural generation rate
            time.sleep(random.uniform(0.1, 1.0))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
        producer.flush()

if __name__ == "__main__":
    run_producer()
