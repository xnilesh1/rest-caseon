import os
from google.cloud.sql.connector import Connector
import sqlalchemy

from dotenv import load_dotenv

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
            connector = Connector()

            # Configure connection according to the MySQL configuration
            connection = sqlalchemy.create_engine(
                'mysql+pymysql://',
                creator=lambda: connector.connect(
                os.getenv("CONNECTION_NAME"),
                'pymysql',
                user=os.getenv("USERNAME"),
                password=os.getenv("PASSWORD"),
                db=os.getenv("DATABASE")
                )
            )
            return connection
        except Exception as e:
            print(f"An error occurred in database connection: {str(e)}")
            return None

def release_connection(connection):
    """Return a connection to the pool"""
    try:
        if connection:
            # For SQLAlchemy Engine objects, we don't need to check 'open'
            if len(_connection_pool) < MAX_POOL_SIZE:
                _connection_pool.append(connection)
            else:
                # For SQLAlchemy Engine objects, we use dispose() instead of close()
                connection.dispose()
    except Exception as e:
        print(f"Error returning connection to pool: {e}")
        try:
            connection.dispose()
        except:
            pass


