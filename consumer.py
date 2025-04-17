from kafka import KafkaProducer
import json
import time
import random
import requests

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
            "timestamp": time.time()
        }
        producer.send('customer-data', value=data)
        print(f"Sent: {data}")
        customer_id_counter += 1  # Increment customer_id
        name_counter += 1  # Increment name counter

    time.sleep(5)  # Wait 5 seconds before fetching the next batch

producer.flush()
