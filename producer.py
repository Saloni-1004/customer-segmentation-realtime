import requests
import random
import time
import json
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Neon Database Connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Initialize Kafka producer with retry logic
def initialize_producer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],  # Verify this address
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Kafka producer initialized successfully.")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1} failed: No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Failed to initialize Kafka producer: {e}")
            time.sleep(5)
    print("Max retries reached. Exiting.")
    exit(1)

producer = initialize_producer()

# Initialize database engine
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # Test connection
        print("Database connection established successfully.")
except Exception as e:
    print(f"Failed to connect to database: {e}")
    exit(1)

def fetch_real_data():
    response = requests.get("https://fakestoreapi.com/users")
    if response.status_code == 200:
        return response.json()
    return []

# Counters for customer_id and name uniqueness
customer_id_counter = 1
name_counter = 1

while True:
    users = fetch_real_data()
    if not users:
        print("Failed to fetch data from API, retrying in 5 seconds...")
        time.sleep(5)
        continue

    for user in users:
        customer_id = str(customer_id_counter)
        unique_name = f"{user['name']['firstname']}_{name_counter}"
        
        data = {
            "customer_id": customer_id,
            "name": unique_name,
            "age": random.randint(18, 65),
            "purchase_amount": round(random.uniform(10, 500), 2),
            "created_at": datetime.now().isoformat()
        }
        
        # Send message to Kafka with retry
        try:
            producer.send('customer-data', value=data)
            producer.flush()  # Ensure message is sent
            print(f"Sent message to Kafka: {data}")
        except NoBrokersAvailable as e:
            print(f"Failed to send message: {e}. Check if Kafka is running on localhost:9092.")
            producer = initialize_producer()  # Reinitialize producer
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")
        
        # Insert data into customer_segments table
        kafka_data = data.copy()
        kafka_data["cluster"] = random.randint(0, 2)
        kafka_data["created_at"] = datetime.now()
        
        with engine.connect() as conn:
            try:
                query = text("""
                    INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                    VALUES (:customer_id, :name, :age, :purchase_amount, :cluster, :created_at)
                """)
                conn.execute(query, parameters=kafka_data)
                conn.commit()
                print(f"Successfully inserted record: {kafka_data}")
            except Exception as e:
                print(f"Insertion failed: {e}")
        
        customer_id_counter += 1
        name_counter += 1
    
    time.sleep(5)
