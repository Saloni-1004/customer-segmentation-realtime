import json
import psycopg2
from kafka import KafkaConsumer
import time
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Consumer script starting...")

# Kafka setup
KAFKA_TOPIC = "customer-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Update if Kafka is deployed elsewhere

# Connect to Kafka
connected_kafka = False
while not connected_kafka:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else {},
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="customer-group"
        )
        logger.info("Kafka consumer initialized successfully.")
        connected_kafka = True
    except Exception as e:
        logger.error(f"Failed to initialize Kafka consumer: {e}")
        logger.info("Retrying in 5 seconds...")
        time.sleep(5)

# Neon PostgreSQL connection with retry
max_retries = 5
retry_delay = 5
conn = None
cursor = None
for attempt in range(max_retries):
    try:
        conn = psycopg2.connect(
            host="ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech",
            port="5432",
            dbname="neondb",
            user="neondb_owner",
            password="npg_5UbnztxlVuD1",
            sslmode='require'
        )
        cursor = conn.cursor()
        logger.info("Connected to Neon PostgreSQL successfully.")
        break
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL (attempt {attempt + 1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            logger.critical("Max retries reached. Exiting.")
            exit(1)

# Create customer_segments table if not exists
try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer_segments (
        record_id SERIAL PRIMARY KEY,
        customer_id TEXT,
        name TEXT,
        age INTEGER,
        purchase_amount DECIMAL(10,2),
        cluster INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    logger.info("customer_segments table ensured.")
except Exception as e:
    logger.error(f"Failed to create customer_segments table: {e}")
    exit(1)

# Simple segmentation logic based on purchase_amount
def determine_cluster(purchase_amount):
    if purchase_amount is None or purchase_amount < 0:
        return 0
    if purchase_amount < 100:
        return 0  # Low spenders
    elif purchase_amount < 300:
        return 1  # Medium spenders
    else:
        return 2  # High spenders

# Start consuming messages
logger.info("Waiting for messages...")

try:
    for message in consumer:
        data = message.value
        logger.info(f"Received message: {data}")

        try:
            # Handle both old (string name) and new (nested name) message formats
            customer_id = data.get("id", data.get("customer_id", ""))
            name_data = data.get("name", {})
            if isinstance(name_data, str):
                name = name_data  # Old format: name is a string
            else:
                name = f"{name_data.get('firstname', '')} {name_data.get('lastname', '')}".strip()  # New format: nested dict
            age = data.get("age", 0)
            purchase_amount = data.get("purchase_amount", 0.0)
            segment = data.get("segment", "Segment-0")
            timestamp = data.get("timestamp", time.time())

            # Determine cluster
            cluster = int(segment.split('-')[1]) if segment and segment.startswith("Segment-") else determine_cluster(purchase_amount)

            cursor.execute(
                """
                INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
                """,
                (customer_id, name, age, purchase_amount, cluster, timestamp)
            )
            conn.commit()
            logger.info(f"[✅] Inserted into customer_segments | Cluster: {cluster} | Customer: {name}")
        except Exception as db_err:
            logger.error(f"[❌ ERROR] DB Insert Failed: {db_err}")
            conn.rollback()

except KeyboardInterrupt:
    logger.info("Consumer stopped by user.")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    if consumer:
        consumer.close()
    logger.info("Resources closed.")
