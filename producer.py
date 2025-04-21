import requests
import random
import time
import json
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime
from kafka import KafkaProducer

# Neon Database Connection
DB_HOST = "ep-dry-violet-a4v38rh7-pooler.us-east-1.aws.neon.tech"
DB_PORT = 5432
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = urllib.parse.quote_plus("npg_5UbnztxlVuD1")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("Kafka producer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")
    exit(1)

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
customer_id_counter = 1  # Start at 1
name_counter = 1

while True:
    users = fetch_real_data()
    if not users:
        print("Failed to fetch data from API, retrying in 5 seconds...")
        time.sleep(5)
        continue

    for user in users:
        customer_id = str(customer_id_counter)  # Simple integer as customer_id (e.g., "1", "2")
        unique_name = f"{user['name']['firstname']}_{name_counter}"  # e.g., "john_1"
        
        data = {
            "customer_id": customer_id,
            "name": unique_name,
            "age": random.randint(18, 65),
            "purchase_amount": round(random.uniform(10, 500), 2),
            "created_at": datetime.now().isoformat()  # ISO format for JSON serialization
        }
        
        # Send message to Kafka
        try:
            producer.send('customer-data', value=data)
            print(f"Sent message to Kafka: {data}")
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")
        
        # Also insert data into customer_segments table (if you want to keep this functionality)
        kafka_data = data.copy()
        kafka_data["cluster"] = random.randint(0, 2)  # Randomly assign cluster 0, 1, or 2
        kafka_data["created_at"] = datetime.now()  # Use datetime object for database
        
        with engine.connect() as conn:
            try:
                query = text("""
                    INSERT INTO customer_segments (customer_id, name, age, purchase_amount, cluster, created_at)
                    VALUES (:customer_id, :name, :age, :purchase_amount, :cluster, :created_at)
                """)
                conn.execute(query, parameters=kafka_data)  # Use parameters= to pass the dictionary
                conn.commit()
                print(f"Successfully inserted record: {kafka_data}")
            except Exception as e:
                print(f"Insertion failed: {e}")
        
        customer_id_counter += 1  # Increment customer_id
        name_counter += 1  # Increment name counter
    
    time.sleep(5)  # Wait 5 seconds before fetching the next batch
