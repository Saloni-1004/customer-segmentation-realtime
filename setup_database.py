import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_database(mode):
    """Set up the database tables for the specified mode"""
    print(f"Setting up {mode} database...")
    
    try:
        # Connect to the database
        if mode == "local":
            conn = psycopg2.connect(
                dbname=os.getenv("LOCAL_DB_NAME"),
                user=os.getenv("LOCAL_DB_USER"),
                password=os.getenv("LOCAL_DB_PASSWORD"),
                host=os.getenv("LOCAL_DB_HOST"),
                port=os.getenv("LOCAL_DB_PORT")
            )
        else:
            conn = psycopg2.connect(
                dbname=os.getenv("DEPLOY_DB_NAME"),
                user=os.getenv("DEPLOY_DB_USER"),
                password=os.getenv("DEPLOY_DB_PASSWORD"),
                host=os.getenv("DEPLOY_DB_HOST"),
                port=os.getenv("DEPLOY_DB_PORT"),
                sslmode=os.getenv("DEPLOY_DB_SSLMODE")
            )
        
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_segments (
            id SERIAL PRIMARY KEY,
            customer_id VARCHAR(255),
            name VARCHAR(255),
            age INTEGER,
            purchase_amount FLOAT,
            cluster INTEGER,
            created_at TIMESTAMP
        );
        """)
        
        print(f"✅ {mode.upper()} database setup complete!")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error setting up {mode} database: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    # Setup both databases
    local_setup = setup_database("local")
    deploy_setup = setup_database("deploy")
    
    if local_setup and deploy_setup:
        print("✅ All databases are set up and ready to use!")
    else:
        print("⚠️ There were issues setting up one or both databases.")
