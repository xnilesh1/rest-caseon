import pymysql
from datetime import datetime
import os
from dotenv import load_dotenv
from pymysql.cursors import DictCursor

load_dotenv()

# Create a connection pool
_connection_pool = []
MAX_POOL_SIZE = 5

def getconnection():
    """Get a database connection from the pool or create a new one if needed"""
    if _connection_pool:
        return _connection_pool.pop()
    else:
        try:
            timeout = 10
            connection = pymysql.connect(
                charset="utf8mb4",
                connect_timeout=timeout,
                cursorclass=DictCursor,
                db="defaultdb",
                host=os.getenv("MYSQL_HOST"),
                password=os.getenv("MYSQL_PASSWORD"),
                read_timeout=timeout,
                port=10849,
                user=os.getenv("MYSQL_USER"),
                write_timeout=timeout,
            )
            return connection
        except Exception as e:
            print(f"An error occurred in database connection: {str(e)}")
            return None

def release_connection(connection):
    """Return a connection to the pool"""
    try:
        if connection and connection.open:
            # Only add back to pool if it's a valid connection
            if len(_connection_pool) < MAX_POOL_SIZE:
                # Reset the connection state
                connection.ping(reconnect=True)
                _connection_pool.append(connection)
            else:
                # If pool is full, close the connection
                connection.close()
    except Exception as e:
        print(f"Error returning connection to pool: {e}")
        try:
            connection.close()
        except:
            pass



