import json
import psycopg2
from kafka import KafkaConsumer

print("[INFO] Script starting...")

# Kafka setup
KAFKA_TOPIC = "customer-topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="customer-group"
    )
    print("[INFO] Kafka consumer initialized.")
except Exception as e:
    print(f"[ERROR] Failed to initialize Kafka consumer: {e}")
    exit()

# Supabase PostgreSQL connection
try:
    conn = psycopg2.connect(
        host="db.fsulfssfgmgxosgpjjiw.supabase.co",
        port="5432",
        dbname="postgres",
        user="postgres",
        password="Adminsaloni@10",  # 🔐 Replace this!
        sslmode='require'
    )
    cursor = conn.cursor()
    print("[INFO] Connected to Supabase PostgreSQL.")
except Exception as e:
    print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
    exit()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    segment TEXT
)
""")
conn.commit()

# Consume messages from Kafka
print("[INFO] Waiting for messages...")
try:
    for message in consumer:
        data = message.value
        print(f"[INFO] Received: {data}")
        try:
            cursor.execute(
                "INSERT INTO customers (name, email, country, segment) VALUES (%s, %s, %s, %s)",
                (data["name"], data["email"], data["country"], data["segment"])
            )
            conn.commit()
            print("[INFO] Inserted into database.")
        except Exception as db_err:
            print(f"[ERROR] Failed to insert into DB: {db_err}")
except KeyboardInterrupt:
    print("[INFO] Consumer stopped.")
finally:
    cursor.close()
    conn.close()
    consumer.close()


