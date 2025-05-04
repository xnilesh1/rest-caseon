import os
import sqlalchemy
import pymysql
import time
import logging
import socket
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a connection pool
_connection_pool = []
MAX_POOL_SIZE = 5
MAX_RETRIES = 5  # Increased from 3
RETRY_DELAY_START = 5  # Increased initial delay
RETRY_BACKOFF_FACTOR = 1.5  # Multiply delay each attempt

def is_port_open(host, port, timeout=1):
    """Check if a port is open on the specified host"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        s.close()

def wait_for_port(host, port, max_wait=60):
    """Wait for a port to be available"""
    logger.info(f"Waiting for port {host}:{port} to be available (max {max_wait}s)...")
    start_time = time.time()
    while time.time() - start_time < max_wait:
        if is_port_open(host, port):
            logger.info(f"Port {host}:{port} is now available")
            return True
        time.sleep(1)
    logger.error(f"Timed out waiting for {host}:{port}")
    return False

def getconnection():
    """Get a database connection from the pool or create a new one if needed"""
    # First, make sure MySQL is available
    host = '127.0.0.1'
    port = 3306
    
    # Check if MySQL port is open
    if not is_port_open(host, port):
        logger.warning(f"MySQL port {host}:{port} not available, waiting...")
        if not wait_for_port(host, port, max_wait=30):
            logger.error(f"MySQL port {host}:{port} never became available")
            return None
    
    if _connection_pool:
        logger.info("Reusing connection from pool")
        return _connection_pool.pop()
    else:
        for retry in range(MAX_RETRIES):
            try:
                logger.info(f"Attempting to create new database connection (attempt {retry+1}/{MAX_RETRIES})")
                
                # Log connection parameters (without sensitive info)
                logger.info(f"Connection parameters: host='{host}', port={port}, user={os.getenv('USERNAME')}, database={os.getenv('DATABASE')}")
                
                # Connect to the local Cloud SQL Auth Proxy
                connection_url = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(
                    user=os.getenv("USERNAME"),
                    password=os.getenv("PASSWORD"),
                    host=host,
                    port=port,
                    db=os.getenv("DATABASE")
                )
                
                # Add connect_args with longer timeout
                connection = sqlalchemy.create_engine(
                    connection_url,
                    connect_args={"connect_timeout": 30}
                )
                
                # Test the connection
                with connection.connect() as conn:
                    conn.execute(sqlalchemy.text("SELECT 1"))
                    logger.info("Database connection successful")
                    
                return connection
            except Exception as e:
                logger.error(f"Database connection error: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    # Calculate delay with exponential backoff
                    delay = RETRY_DELAY_START * (RETRY_BACKOFF_FACTOR ** retry)
                    logger.info(f"Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
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


