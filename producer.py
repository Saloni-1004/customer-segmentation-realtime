from kafka import KafkaProducer
import json
import time
import random
import requests

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[INFO] Kafka producer initialized successfully")
except Exception as e:
    print(f"[ERROR] Failed to initialize Kafka producer: {e}")
    exit(1)

def fetch_real_data():
    """Fetch customer data from Fake Store API"""
    try:
        response = requests.get("https://fakestoreapi.com/users")
        if response.status_code == 200:
            return response.json()
        print(f"[WARNING] API returned status code {response.status_code}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to fetch data: {e}")
        return []

# Counters for customer_id and name uniqueness
customer_id_counter = 1
name_counter = 1

print("[INFO] Starting data production loop...")
while True:
    users = fetch_real_data()
    if not users:
        print("[WARNING] Failed to fetch data from API, retrying in 5 seconds...")
        time.sleep(5)
        continue

    for user in users:
        customer_id = str(customer_id_counter)
        unique_name = f"{user['name']['firstname']}_{name_counter}"
        
        # Create customer data
        data = {
            "customer_id": customer_id,
            "name": unique_name,
            "email": user['email'],  # Include email from API
            "country": "US",  # Default country
            "age": random.randint(18, 65),
            "purchase_amount": round(random.uniform(10, 500), 2),
            "segment": f"Segment-{random.randint(0, 2)}",  # Add segment field
            "timestamp": time.time()
        }
        
        # Send to Kafka
        try:
            producer.send('customer-data', value=data)
            print(f"[INFO] Sent: {data}")
        except Exception as e:
            print(f"[ERROR] Failed to send message: {e}")
            
        customer_id_counter += 1
        name_counter += 1
    
    time.sleep(5)  # Wait 5 seconds before fetching the next batch

producer.flush()
