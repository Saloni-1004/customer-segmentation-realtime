from kafka import KafkaProducer
import json
import time
import requests
import logging

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

logger.info("Starting data production loop...")
while True:
    users = fetch_fake_store_api()
    
    if not users:
        logger.warning("Failed to fetch data from Fake Store API, retrying in 5 seconds...")
        time.sleep(5)
        continue
    
    for user in users:
        try:
            # Prepare data from Fake Store API
            data = {
                "id": str(user.get("id", 0)),
                "customer_id": str(user.get("id", 0)),  # Use ID as customer_id
                "name": {"firstname": user["name"]["firstname"], "lastname": user["name"]["lastname"]},
                "email": user.get("email", ""),
                "age": 0,  # Fake Store API doesn't provide age, set to 0 or fetch from another source if available
                "purchase_amount": round(abs(hash(user["email"])) % 1000 / 10, 2),  # Generate pseudo-random purchase amount
                "segment": f"Segment-{abs(hash(user['email'])) % 3}",  # Assign random segment
                "timestamp": time.time()
            }
            
            # Send to Kafka
            producer.send('customer-data', value=data)
            logger.info(f"Sent: Customer ID: {data['customer_id']}, Name: {data['name']['firstname']} {data['name']['lastname']}, Amount: ${data['purchase_amount']}")
        except Exception as e:
            logger.error(f"Error processing user data: {e}")
    
    # Wait before fetching the next batch
    logger.info(f"Processed {len(users)} users. Waiting 5 seconds before next batch...")
    time.sleep(5)

producer.flush()
