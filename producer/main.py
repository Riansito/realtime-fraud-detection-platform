import json
import time
import uuid
import random
import os
from datetime import datetime, timezone
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

def generate_transaction(account_id=None, customer_id=None):
    """Generates a procedural transaction event with synthetic anomalies."""
    is_anomaly = random.random() < 0.05
    anomaly_type = random.choice(["HIGH_AMOUNT", "NIGHT_TIME"]) if is_anomaly else None
    
    amount = round(random.uniform(5.0, 5000.0), 2)
    if anomaly_type == "HIGH_AMOUNT":
        amount = round(random.uniform(20000.0, 100000.0), 2)
        
    event_time = datetime.now(timezone.utc)
    if anomaly_type == "NIGHT_TIME":
        event_time = event_time.replace(hour=random.randint(1, 4))
        
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id or f"ACC-{random.randint(1000, 9999)}",
        "customer_id": customer_id or f"CUST-{random.randint(100, 999)}",
        "merchant_id": f"MERCH-{random.randint(1000, 9999)}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": str(uuid.uuid4()),
        "ip_address": fake.ipv4(),
        "region": random.choice(REGIONS),
        "event_time": event_time.isoformat()
    }

def delivery_report(err, msg):
    """Callback triggered on successful/failed delivery."""
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        pass # To reduce log spam, you can print if you want

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
            # 2% chance of Velocity Anomaly (many transactions in a short time)
            if random.random() < 0.02:
                vuln_account = f"ACC-{random.randint(1000, 9999)}"
                vuln_customer = f"CUST-{random.randint(100, 999)}"
                print(f"⚠️ Injecting VELOCITY ANOMALY for {vuln_account}")
                for _ in range(random.randint(5, 12)):
                    tx = generate_transaction(account_id=vuln_account, customer_id=vuln_customer)
                    producer.produce(KAFKA_TOPIC, key=tx["region"], value=json.dumps(tx), callback=delivery_report)
                    producer.poll(0)
            else:
                tx = generate_transaction()
                if tx["amount"] > 15000.0:
                    print(f"⚠️ Injecting HIGH_AMOUNT ANOMALY for {tx['account_id']}: {tx['amount']}")
                
                producer.produce(KAFKA_TOPIC, key=tx["region"], value=json.dumps(tx), callback=delivery_report)
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
