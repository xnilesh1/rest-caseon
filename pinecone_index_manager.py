import os
import logging
import pymysql
from dotenv import load_dotenv
from typing import Tuple, Optional
import uuid
import time
from functools import wraps
from pinecone import Pinecone
from pinecone import ServerlessSpec
from connection import getconnection, release_connection
import sqlalchemy


load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

def retry_on_exception(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, 
                      exceptions_to_retry: tuple = (Exception,)):
    """
    Retry decorator for functions that might experience transient failures.
    
    Args:
        max_retries (int): Maximum number of retries
        delay (float): Initial delay between retries in seconds
        backoff (float): Multiplier for delay between retries
        exceptions_to_retry (tuple): Exception types to retry on
        
    Returns:
        Callable: Decorated function with retry logic
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for retry in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    last_exception = e
                    if retry == max_retries:
                        logger.error(f"Max retries ({max_retries}) reached for {func.__name__}")
                        raise
                    
                    logger.warning(f"Retry {retry+1}/{max_retries} for {func.__name__} after error: {str(e)}")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            # This should not be reached due to the raise in the except block
            raise last_exception
        
        return wrapper
    
    return decorator

def check_environment_requirements() -> bool:
    """
    Check if all required environment variables are set.
    This should be called during module initialization to fail fast.
    
    Returns:
        bool: True if all required variables are present, False otherwise
    """
    required_vars = [
        "PINECONE_API_KEY",
        "PINECONE_API_KEY_SECOND_PROJECT",
        # Add other required env vars
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    return True

# Check environment requirements on module import
env_check_result = check_environment_requirements()
if not env_check_result:
    logger.warning("Environment check failed - some functionality may not work correctly")

def insert_case(namespace: str, index_name: str, project: str) -> bool:
    """
    Insert a case record into the volume_handling_table.
    
    Args:
        namespace (str): The namespace identifier for the case
        index_name (str): The Pinecone index name
        project (str): The project identifier
        
    Returns:
        bool: True if insert was successful, False otherwise
    """
    if not all([namespace, index_name, project]):
        logger.error("Missing required parameters for insert_case")
        return False
        
    conn = None
    try:
        conn = getconnection()
        if not conn:
            logger.error("Failed to connect to database")
            return False
            
        # Using SQLAlchemy connection
        sql = sqlalchemy.text("INSERT INTO volume_handling_table (namespace, index_name, project) VALUES (:namespace, :index_name, :project)")
        with conn.connect() as connection:
            connection.execute(sql, {"namespace": namespace, "index_name": index_name, "project": project})
            connection.commit()
        logger.info(f"Successfully inserted case with namespace: {namespace}")
        return True
    except Exception as e:
        if isinstance(e, pymysql.err.IntegrityError) or "Duplicate entry" in str(e):
            logger.warning(f"Duplicate namespace detected: {namespace}")
        else:
            logger.error(f"Database error in insert_case: {str(e)}")
        return False
    finally:
        if conn:
            release_connection(conn)


@retry_on_exception(max_retries=3, delay=1.0, backoff=2.0)
def create_unique_pinecone_index(dimension: int, metric: str = "cosine", api_key: Optional[str] = None) -> str:
    """
    Create a new Pinecone index with a unique auto-generated name to avoid naming collisions.
    
    Args:
        dimension (int): Vector dimension for the index
        metric (str): Distance metric to use (default: "cosine")
        api_key (str, optional): Pinecone API key, defaults to environment variable
        
    Returns:
        str: The name of the created index
        
    Raises:
        ValueError: If dimension is invalid or API key is missing
        Exception: If index creation fails
    """
    if dimension <= 0:
        logger.error(f"Invalid dimension value: {dimension}")
        raise ValueError("Dimension must be a positive integer")
        
    # Get API key from environment if not provided
    actual_api_key = api_key or os.environ.get("PINECONE_API_KEY_FIRST_PROJECT")
    if not actual_api_key:
        logger.error("No Pinecone API key provided")
        raise ValueError("Pinecone API key is required")
    
    pc = None
    try:
        pc = Pinecone(api_key=actual_api_key)
        existing_indexes = pc.list_indexes()
        
        # Generate a unique index name
        max_attempts = 5
        attempts = 0
        while attempts < max_attempts:
            index_name = f"index-{uuid.uuid4().hex[:8]}"
            if index_name not in existing_indexes:
                break
            attempts += 1
            
        if attempts >= max_attempts:
            logger.error("Failed to generate unique index name after multiple attempts")
            raise Exception("Could not generate unique index name")
                
        # Create the index
        pc.create_index(
            name=index_name,
            dimension=dimension,
            metric=metric,
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1"
            ),
            deletion_protection="disabled"
        )
        logger.info(f"Successfully created Pinecone index: {index_name}")
        return index_name
    except Exception as e:
        logger.error(f"Error creating Pinecone index: {str(e)}")
        raise
    finally:
        # No need to explicitly close Pinecone client
        pass


@retry_on_exception(max_retries=3, delay=0.5, backoff=1.5)
def count_namespaces_in_index(index_name: str, api_key: Optional[str] = None) -> int:
    """
    Count the number of namespaces in the given Pinecone index.
    
    Args:
        index_name (str): Name of the Pinecone index
        api_key (str, optional): Pinecone API key, defaults to environment variable
        
    Returns:
        int: Number of namespaces in the index
        
    Raises:
        ValueError: If index_name is empty or API key is missing
        Exception: If index stats retrieval fails
    """
    if not index_name:
        logger.error("Empty index_name provided")
        raise ValueError("index_name cannot be empty")
        
    # Get API key from environment if not provided
    actual_api_key = api_key or os.environ.get("PINECONE_API_KEY_FIRST_PROJECT")
    if not actual_api_key:
        logger.error("No Pinecone API key provided")
        raise ValueError("Pinecone API key is required")
    
    pc = None
    index = None
    try:
        pc = Pinecone(api_key=actual_api_key)
        index = pc.Index(index_name)
        stats = index.describe_index_stats()
        namespace_count = len(stats.get("namespaces", {}))
        logger.debug(f"Index {index_name} has {namespace_count} namespaces")
        return namespace_count
    except Exception as e:
        logger.error(f"Error retrieving namespace count for index {index_name}: {str(e)}")
        raise
    finally:
        # No explicit cleanup needed for Pinecone clients
        pass


def get_index_project_by_namespace(namespace: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the index_name and project values for a given namespace from the database.
    
    Args:
        namespace (str): The namespace to look up
        
    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing (index_name, project)
        If no matching record is found, returns (None, None)
    """
    if not namespace:
        logger.error("Empty namespace provided")
        return None, None
        
    conn = None
    try:
        conn = getconnection()
        if not conn:
            logger.error("Failed to connect to database")
            return None, None
            
        # Using SQLAlchemy connection
        sql = sqlalchemy.text("SELECT index_name, project FROM volume_handling_table WHERE namespace = :namespace")
        with conn.connect() as connection:
            result = connection.execute(sql, {"namespace": namespace}).fetchone()
            
            if result:
                index_name = result[0]  # Access by index instead of dict
                project = result[1]
                logger.debug(f"Found index_name: {index_name}, project: {project} for namespace: {namespace}")
                return index_name, project
            logger.warning(f"No record found for namespace: {namespace}")
            return None, None
            
    except Exception as e:
        logger.error(f"Error querying database for namespace {namespace}: {str(e)}")
        return None, None
    finally:
        if conn:
            release_connection(conn)


def get_index_namespace_and_project(index_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the namespace and project values for a given index_name from the database.
    
    Args:
        index_name (str): The index_name to look up
        
    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing (namespace, project)
        If no matching record is found, returns (None, None)
    """
    if not index_name:
        logger.error("Empty index_name provided")
        return None, None
        
    conn = None
    try:
        conn = getconnection()
        if not conn:
            logger.error("Failed to connect to database")
            return None, None
            
        # Using SQLAlchemy connection
        sql = sqlalchemy.text("SELECT namespace, project FROM volume_handling_table WHERE index_name = :index_name")
        with conn.connect() as connection:
            result = connection.execute(sql, {"index_name": index_name}).fetchone()
            
            if result:
                namespace = result[0]  # Access by index instead of dict
                project = result[1]
                logger.debug(f"Found namespace: {namespace}, project: {project} for index: {index_name}")
                return namespace, project
            logger.warning(f"No record found for index_name: {index_name}")
            return None, None
            
    except Exception as e:
        logger.error(f"Error querying database for index {index_name}: {str(e)}")
        return None, None
    finally:
        if conn:
            release_connection(conn)


def ping_pinecone(api_key: Optional[str] = None) -> bool:
    """
    Check if Pinecone service is reachable and API key is valid.
    Useful for health checks.
    
    Args:
        api_key (str, optional): Pinecone API key to validate
        
    Returns:
        bool: True if Pinecone is reachable and API key is valid, False otherwise
    """
    actual_api_key = api_key or os.environ.get("PINECONE_API_KEY_FIRST_PROJECT")
    if not actual_api_key:
        logger.error("No Pinecone API key provided for health check")
        return False
        
    try:
        pc = Pinecone(api_key=actual_api_key)
        # Just list indexes to see if the API is responsive
        pc.list_indexes()
        return True
    except Exception as e:
        logger.error(f"Pinecone health check failed: {str(e)}")
        return False

def ping_database() -> bool:
    """
    Check if database connection is working.
    Useful for health checks.
    
    Returns:
        bool: True if database is reachable, False otherwise
    """
    conn = None
    try:
        conn = getconnection()
        if not conn:
            logger.error("Failed to connect to database during health check")
            return False
            
        # Simple query to test connection
        with conn.connect() as connection:
            connection.execute(sqlalchemy.text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        return False
    finally:
        if conn:
            release_connection(conn)

