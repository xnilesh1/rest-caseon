import os
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
import json
from google.oauth2 import service_account

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
            # Load service account credentials if provided
            service_account_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
            if service_account_json:
                info = json.loads(service_account_json)
                creds = service_account.Credentials.from_service_account_info(info)
            else:
                creds = None

            # Determine IP type for connection
            ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

            # Initialize the Cloud SQL connector
            connector = Connector(
                credentials=creds,
                ip_type=ip_type,
                refresh_strategy="LAZY",
            )

            # Create the SQLAlchemy engine
            engine = sqlalchemy.create_engine(
                "mysql+pymysql://",
                creator=lambda: connector.connect(
                    os.environ.get("INSTANCE_CONNECTION_NAME"),
                    "pymysql",
                    user=os.environ.get("DB_USER"),
                    password=os.environ.get("DB_PASS"),
                    db=os.environ.get("DB_NAME"),
                ),
            )
            return engine
        except Exception as e:
            print(f"An error occurred in database connection: {e}")
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


