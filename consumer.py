import json
import psycopg2
from kafka import KafkaConsumer, TopicPartition
import time
import logging
import os
from kafka.errors import KafkaError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Consumer script starting...")

# Kafka setup
KAFKA_TOPIC = "customer-data"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"  # Force IPv4 to avoid IPv6 issues

# Connect to Kafka with topic existence check
connected_kafka = False
max_retries = 10
retry_delay = 5
while not connected_kafka and max_retries > 0:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else {},
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="customer-group",
            api_version=(3, 9, 0)  # Match your Kafka server version
        )
        
        # Check if topic exists
        topics = consumer.topics()
        if KAFKA_TOPIC not in topics:
            logger.warning(f"Topic {KAFKA_TOPIC} not found. Waiting for topic creation...")
            time.sleep(retry_delay)
            max_retries -= 1
            continue
        
        # Subscribe to topic first, then wait for assignment
        consumer.subscribe([KAFKA_TOPIC])
        
        # Poll to trigger partition assignment
        consumer.poll(timeout_ms=5000)
        
        # Now partitions should be assigned
        if not consumer.assignment():
            logger.warning("No partitions assigned yet. Waiting...")
            time.sleep(retry_delay)
            max_retries -= 1
            continue
        
        logger.info("Kafka consumer initialized successfully.")
        connected_kafka = True
    except KafkaError as e:
        logger.error(f"Failed to initialize Kafka consumer: {e}")
        logger.info(f"Retrying in {retry_delay} seconds... ({max_retries} attempts left)")
        time.sleep(retry_delay)
        max_retries -= 1
    except Exception as e:
        logger.error(f"Unexpected error during Kafka connection: {e}")
        time.sleep(retry_delay)
        max_retries -= 1

if not connected_kafka:
    logger.critical("Failed to connect to Kafka after maximum retries. Exiting.")
    exit(1)

# Neon PostgreSQL connection with retry
max_retries_db = 5
retry_delay_db = 5
conn = None
cursor = None
for attempt in range(max_retries_db):
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
        logger.error(f"Failed to connect to PostgreSQL (attempt {attempt + 1}/{max_retries_db}): {e}")
        if attempt < max_retries_db - 1:
            time.sleep(retry_delay_db)
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

# Updated segmentation logic to match producer exactly
def determine_cluster(purchase_amount):
    if purchase_amount is None or purchase_amount < 0:
        return 0
    if purchase_amount < 100:
        return 0  # Low spenders
    elif purchase_amount < 500:
        return 1  # Medium spenders
    else:
        return 2  # High spenders

# Function to safely handle purchase amounts to prevent DB overflow
def safe_purchase_amount(amount):
    try:
        # Convert to float if it's a string
        if isinstance(amount, str):
            amount = float(amount)
        
        # Ensure amount is a number and within DB limits
        if amount is None or not isinstance(amount, (int, float)):
            return 0.0
        
        # Keep within PostgreSQL DECIMAL(10,2) limits (max: 99999999.99)
        return min(float(amount), 999999.99)
    except (ValueError, TypeError) as e:
        logger.error(f"Error converting purchase amount: {e}")
        return 0.0

# Start consuming messages
logger.info("Waiting for messages...")

try:
    for message in consumer:
        data = message.value
        logger.info(f"Received message: {data}")

        try:
            # Extract customer_id - handle both formats
            customer_id = data.get("customer_id", data.get("id", ""))
            
            # Handle name - support both string and nested dict formats
            name_data = data.get("name", {})
            if isinstance(name_data, str):
                name = name_data  # Old format: name is a string
            else:
                # New format: extract from nested dict
                firstname = name_data.get("firstname", "")
                lastname = name_data.get("lastname", "")
                name = f"{firstname} {lastname}".strip()
                if not name:
                    name = f"Customer-{customer_id}"
            
            # Safe extraction of other fields
            age = int(data.get("age", 0)) if data.get("age") is not None else 0
            
            # Safely process purchase amount to prevent overflow
            purchase_amount = safe_purchase_amount(data.get("purchase_amount", 0.0))
            
            # Extract segment & timestamp
            segment = data.get("segment", "")
            timestamp = data.get("timestamp", time.time()) 
            
            # Determine cluster
            cluster = int(segment.split('-')[1]) if segment and segment.startswith("Segment-") else determine_cluster(purchase_amount)

            # Insert into database with safe values
            cursor.execute(
                """
                INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
                """,
                (str(customer_id), name, age, purchase_amount, cluster, timestamp)
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
