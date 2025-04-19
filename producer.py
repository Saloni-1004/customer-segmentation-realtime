from kafka import KafkaProducer
import json
import time
import requests
import logging
import random
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Update if Kafka is deployed elsewhere
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    exit(1)

def fetch_fake_store_api():
    """Fetch customer data from Fake Store API"""
    try:
        response = requests.get("https://fakestoreapi.com/users", timeout=10)
        if response.status_code == 200:
            users = response.json()
            logger.info(f"Successfully fetched {len(users)} users from Fake Store API")
            return users
        logger.warning(f"API returned status code {response.status_code}")
        return []
    except Exception as e:
        logger.error(f"Failed to fetch data from Fake Store API: {e}")
        return []

# Simulate realistic customer data
def generate_customer_data(user):
    customer_id = str(user.get("id", 0))
    name = {"firstname": user["name"]["firstname"], "lastname": user["name"]["lastname"]}
    
    # Realistic age between 18 and 80
    age = random.randint(18, 80)
    
    # Realistic purchase amount with slight variation and occasional large purchase
    base_amount = random.uniform(10, 1000)
    purchase_amount = round(base_amount + random.uniform(-50, 50), 2)  # Add some fluctuation
    if random.random() < 0.1:  # 10% chance of a large purchase
        purchase_amount = round(purchase_amount + random.uniform(1000, 5000), 2)
    
    # Segment based on purchase amount
    if purchase_amount < 100:
        segment = "Segment-0"  # Low spenders
    elif purchase_amount < 500:
        segment = "Segment-1"  # Medium spenders
    else:
        segment = "Segment-2"  # High spenders
    
    return {
        "id": customer_id,
        "customer_id": customer_id,
        "name": name,
        "email": user.get("email", ""),
        "age": age,
        "purchase_amount": purchase_amount,
        "segment": segment,
        "timestamp": time.time()
    }

logger.info("Starting data production loop...")
while True:
    users = fetch_fake_store_api()
    
    if not users:
        logger.warning("Failed to fetch data from Fake Store API, retrying in 5 seconds...")
        time.sleep(5)
        continue
    
    for user in users:
        try:
            data = generate_customer_data(user)
            producer.send('customer-data', value=data)
            logger.info(f"Sent: Customer ID: {data['customer_id']}, Name: {data['name']['firstname']} {data['name']['lastname']}, Age: {data['age']}, Amount: ${data['purchase_amount']}")
        except Exception as e:
            logger.error(f"Error processing user data: {e}")
    
    logger.info(f"Processed {len(users)} users. Waiting 5 seconds before next batch...")
    time.sleep(5)

producer.flush()
