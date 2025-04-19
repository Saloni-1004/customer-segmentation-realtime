import json
import psycopg2
from kafka import KafkaConsumer
import time
import logging
import os
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Consumer script starting...")

# Kafka setup
KAFKA_TOPIC = "customer-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Connect to Kafka
connected_kafka = False
while not connected_kafka:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
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

# PostgreSQL connection (Neon)
max_retries = 5
retry_delay = 5
conn = None
cursor = None
for attempt in range(max_retries):
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432"),
            dbname=os.getenv("PGDATABASE", "neondb"),
            user=os.getenv("PGUSER", "neondb_owner"),
            password=os.getenv("PGPASSWORD"),
            sslmode='require'
        )
        cursor = conn.cursor()
        logger.info("Connected to Neon PostgreSQL successfully.")
        break
    except Exception as e:
        logger.error(f"PostgreSQL connection failed (attempt {attempt + 1}): {e}")
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            logger.critical("Max retries reached. Exiting.")
            exit(1)

# Create table if not exists
try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer_segments (
        record_id SERIAL PRIMARY KEY,
        customer_id TEXT,
        name TEXT,
        age INTEGER,
        purchase_amount DECIMAL(10,2),
        cluster INTEGER,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    logger.info("customer_segments table ensured.")
except Exception as e:
    logger.error(f"Failed to create customer_segments table: {e}")
    exit(1)

# Cluster logic
def determine_cluster(purchase_amount):
    if purchase_amount is None:
        return 0
    if purchase_amount < 100:
        return 0
    elif purchase_amount < 300:
        return 1
    else:
        return 2

# Start consuming
logger.info("Waiting for messages...")
try:
    for message in consumer:
        data = message.value
        logger.info(f"Received message: {data}")

        try:
            segment = data.get("segment", "Segment-0")
            cluster = int(segment.split('-')[1]) if segment.startswith("Segment-") else determine_cluster(data.get("purchase_amount", 0))

            created_at = datetime.now(timezone.utc)  # ✅ Correct UTC datetime

            cursor.execute(
                """
                INSERT INTO customer_segments
                (customer_id, name, age, purchase_amount, cluster, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    data.get("customer_id", ""),
                    data.get("name", ""),
                    data.get("age", 0),
                    data.get("purchase_amount", 0.0),
                    cluster,
                    created_at
                )
            )
            conn.commit()
            logger.info(f"[✅] Inserted into customer_segments | Cluster: {cluster}")

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
