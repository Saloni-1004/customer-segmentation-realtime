from kafka import KafkaProducer
import json
import time
import requests
import logging
import random
from datetime import datetime
import os
import pickle

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    exit(1)

# File to store persistent customer data
CUSTOMER_DATA_FILE = 'customer_profiles.pkl'

# Load or initialize customer profiles dictionary
customer_profiles = {}
if os.path.exists(CUSTOMER_DATA_FILE):
    try:
        with open(CUSTOMER_DATA_FILE, 'rb') as f:
            customer_profiles = pickle.load(f)
        logger.info(f"Loaded {len(customer_profiles)} existing customer profiles")
    except Exception as e:
        logger.error(f"Error loading customer profiles: {e}")

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

# Generate realistic customer data
def generate_customer_data(user):
    # Generate a unique customer_id with a chance for new customers
    base_id = str(user.get("id", 0))
    if random.random() < 0.2:  # 20% chance for a new customer
        customer_id = str(int(time.time()) + random.randint(1000, 9999))  # Unique ID
    else:
        customer_id = base_id

    # Check if this customer already exists
    if customer_id in customer_profiles:
        profile = customer_profiles[customer_id]
        # Realistic purchase amount update (-10% to +20% change)
        previous_amount = profile['purchase_amount']
        change_percent = random.uniform(-0.10, 0.20)
        new_amount = round(max(10.0, previous_amount * (1 + change_percent)), 2)  # Cap at $10 min
        # Occasional big purchase (5% chance, up to $2000)
        if random.random() < 0.05:
            new_amount = round(random.uniform(500, 2000), 2)
    else:
        firstname = user["name"]["firstname"]
        lastname = user["name"]["lastname"]
        # Realistic age based on API data or random if new
        age = user.get("dob", {}).get("age", random.randint(18, 80))
        # Realistic initial purchase amount
        new_amount = round(random.uniform(10, 2000), 2)

    # Determine segment based on purchase amount
    if new_amount < 100:
        segment = "Segment-0"  # Low spenders
    elif new_amount < 500:
        segment = "Segment-1"  # Medium spenders
    else:
        segment = "Segment-2"  # High spenders

    # Update or create profile
    profile = {
        "customer_id": customer_id,
        "name": {"firstname": firstname, "lastname": lastname},
        "age": age,
        "purchase_amount": new_amount,
        "segment": segment,
        "timestamp": time.time()
    }
    customer_profiles[customer_id] = profile

    return profile

# Function to save customer profiles periodically
def save_customer_profiles():
    try:
        with open(CUSTOMER_DATA_FILE, 'wb') as f:
            pickle.dump(customer_profiles, f)
        logger.info(f"Saved {len(customer_profiles)} customer profiles")
    except Exception as e:
        logger.error(f"Error saving customer profiles: {e}")

logger.info("Starting data production loop...")
save_counter = 0

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
    
    # Save customer profiles periodically (every 5 batches)
    save_counter += 1
    if save_counter >= 5:
        save_customer_profiles()
        save_counter = 0
    
    logger.info(f"Processed {len(users)} users. Waiting 5 seconds before next batch...")
    time.sleep(5)

producer.flush()
