from kafka import KafkaProducer
import json
import time
import random
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Kafka producer - running locally
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
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
            logger.info(f"Successfully fetched {len(response.json())} users from Fake Store API")
            return response.json()
        logger.warning(f"API returned status code {response.status_code}")
        return []
    except Exception as e:
        logger.error(f"Failed to fetch data from Fake Store API: {e}")
        return []

# Counters for customer_id and name uniqueness
customer_id_counter = 1
name_counter = 1

logger.info("Starting data production loop...")
while True:
    users = fetch_fake_store_api()
    
    if not users:
        logger.warning("Failed to fetch data from Fake Store API, retrying in 5 seconds...")
        time.sleep(5)
        continue
    
    for user in users:
        try:
            customer_id = str(customer_id_counter)
            # Safely extract first name
            firstname = user.get('name', {}).get('firstname', f"User{customer_id}")
            unique_name = f"{firstname}_{name_counter}"
            
            # Create customer data with values from API when available
            data = {
                "customer_id": customer_id,
                "name": unique_name,
                "email": user.get('email', f"{unique_name.lower()}@example.com"),
                "country": "US",  # Default country
                "age": random.randint(18, 65),  # Age not provided by API, so we generate it
                "purchase_amount": round(random.uniform(10, 500), 2),  # Generate random purchase amount
                "segment": f"Segment-{random.randint(0, 2)}",  # Random segment
                "timestamp": time.time()
            }
            
            # Send to Kafka
            producer.send('customer-data', value=data)
            logger.info(f"Sent: Customer ID: {data['customer_id']}, Name: {data['name']}, Amount: ${data['purchase_amount']}")
            
            customer_id_counter += 1
            name_counter += 1
        except Exception as e:
            logger.error(f"Error processing user data: {e}")
    
    # Wait before fetching the next batch
    logger.info(f"Processed {len(users)} users. Waiting 5 seconds before next batch...")
    time.sleep(5)

producer.flush()
