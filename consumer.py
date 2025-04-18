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
        password="npg_N4VDg6bocCul",
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

# Consume messages from Kafka and insert into PostgreSQL
try:
    for message in consumer:
        # Extract data from Kafka message
        customer_data = message.value

        # Example of inserting data into PostgreSQL
        cursor.execute("""
        INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster)
        VALUES (%s, %s, %s, %s, %s)
        """, (
            customer_data["customer_id"],
            customer_data["name"],
            customer_data["age"],
            customer_data["purchase_amount"],
            customer_data["cluster"]
        ))
        conn.commit()  # Commit the insert to the database
        print(f"[INFO] Inserted data for customer_id: {customer_data['customer_id']}")

except Exception as e:
    print(f"[ERROR] Failed to consume message from Kafka or insert into PostgreSQL: {e}")
    exit(1)
