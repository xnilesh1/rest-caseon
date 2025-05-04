import os
import sqlalchemy
import pymysql
import time
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a connection pool
_connection_pool = []
MAX_POOL_SIZE = 5
MAX_RETRIES = 3
RETRY_DELAY = 2

def getconnection():
    """Get a database connection from the pool or create a new one if needed"""
    if _connection_pool:
        logger.info("Reusing connection from pool")
        return _connection_pool.pop()
    else:
        for retry in range(MAX_RETRIES):
            try:
                logger.info(f"Attempting to create new database connection (attempt {retry+1}/{MAX_RETRIES})")
                
                # Log connection parameters (without sensitive info)
                logger.info(f"Connection parameters: host='127.0.0.1', port=3306, user={os.getenv('USERNAME')}, database={os.getenv('DATABASE')}")
                
                # Connect to the local Cloud SQL Auth Proxy
                connection = sqlalchemy.create_engine(
                    'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(
                        user=os.getenv("USERNAME"),
                        password=os.getenv("PASSWORD"),
                        host='127.0.0.1',  # Connect to local proxy
                        port=3306,         # Default MySQL port
                        db=os.getenv("DATABASE")
                    )
                )
                
                # Test the connection
                with connection.connect() as conn:
                    conn.execute(sqlalchemy.text("SELECT 1"))
                    logger.info("Database connection successful")
                    
                return connection
            except Exception as e:
                logger.error(f"Database connection error: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error("All connection attempts failed")
                    return None

def release_connection(connection):
    """Return a connection to the pool"""
    try:
        if connection:
            # For SQLAlchemy Engine objects, we don't need to check 'open'
            if len(_connection_pool) < MAX_POOL_SIZE:
                _connection_pool.append(connection)
                logger.info("Connection returned to pool")
            else:
                # For SQLAlchemy Engine objects, we use dispose() instead of close()
                connection.dispose()
                logger.info("Pool full, connection disposed")
    except Exception as e:
        logger.error(f"Error returning connection to pool: {e}")
        try:
            connection.dispose()
        except:
            pass


