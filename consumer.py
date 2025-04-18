import json
import psycopg2
from kafka import KafkaConsumer
import time
import logging

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

# Neon PostgreSQL connection with retry
max_retries = 5
retry_delay = 5
conn = None
cursor = None
for attempt in range(max_retries):
    try:
        conn = psycopg2.connect(
            host="ep-divine-credit-a4zo7ml-pooler.us-east-1.aws.neon.tech",
            port="5432",
            dbname="neondb",
            user="neondb_owner",
            password="npg_dzr6wpmcY8AM",  # Replace with new password if reset
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

# Simple segmentation logic
def determine_cluster(purchase_amount):
    if purchase_amount is None:
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
        logger.info(f"Received: {data}")

        try:
            # Determine customer cluster based on purchase_amount
            cluster = determine_cluster(data.get("purchase_amount", 0))
            # Use current timestamp if data.get("timestamp") is missing or invalid
            timestamp = data.get("timestamp")
            if not timestamp or not isinstance(timestamp, (int, float, str)):
                timestamp = time.time()
            cursor.execute(
                """
                INSERT INTO customer_segments
                (customer_id, name, age, purchase_amount, cluster, created_at)
                VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
                """,
                (
                    data.get("customer_id", ""),
                    data.get("name", ""),
                    data.get("age", 0),
                    data.get("purchase_amount", 0.0),
                    cluster,
                    timestamp
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
