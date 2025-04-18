import json
import psycopg2
from kafka import KafkaConsumer
import time

print("[INFO] Consumer script starting...")

# Kafka setup
KAFKA_TOPIC = "customer-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Connect to Kafka
connected = False
while not connected:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="customer-group"
        )
        print("[INFO] Kafka consumer initialized successfully.")
        connected = True
    except Exception as e:
        print(f"[ERROR] Failed to initialize Kafka consumer: {e}")
        print("[INFO] Retrying in 5 seconds...")
        time.sleep(5)

# Neon PostgreSQL connection
try:
    conn = psycopg2.connect(
        host="ep-divine-credit-a4zo7ml-pooler.us-east-1.aws.neon.tech",
        port="5432",
        dbname="neondb",
        user="neondb_owner",
        password="npg_dzr6wpmcY8AM",
        sslmode='require'
    )
    cursor = conn.cursor()
    print("[INFO] Connected to Neon PostgreSQL.")
except Exception as e:
    print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
    exit(1)

# Create only customer_segments table if not exists
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
    conn.commit()  # Commit the table creation
    print("[INFO] customer_segments table ensured.")
except Exception as e:
    print(f"[ERROR] Failed to create customer_segments table: {e}")
    exit(1)

# Simple segmentation logic
def determine_cluster(purchase_amount):
    if purchase_amount < 100:
        return 0  # Low spenders
    elif purchase_amount < 300:
        return 1  # Medium spenders
    else:
        return 2  # High spenders

# Start consuming messages
print("[INFO] Waiting for messages...")

try:
    for message in consumer:
        data = message.value
        print(f"[INFO] Received: {data}")

        try:
            # Determine customer cluster based on purchase_amount
            cluster = determine_cluster(data.get("purchase_amount", 0))
            cursor.execute(
                """
                INSERT INTO customer_segments
                (customer_id, name, age, purchase_amount, cluster, created_at)
                VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
                """,
                (
                    data.get("customer_id"),
                    data.get("name"),
                    data.get("age"),
                    data.get("purchase_amount"),
                    cluster,
                    data.get("timestamp")
                )
            )

            conn.commit()
            print(f"[✅] Inserted into customer_segments | Cluster: {cluster}")

        except Exception as db_err:
            print(f"[❌ ERROR] DB Insert Failed: {db_err}")
            conn.rollback()

except KeyboardInterrupt:
    print("[INFO] Consumer stopped by user.")
except Exception as e:
    print(f"[ERROR] Unexpected error: {e}")
finally:
    cursor.close()
    conn.close()
    consumer.close()
    print("[INFO] Resources closed.")
