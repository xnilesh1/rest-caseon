import os
import pymysql
from dotenv import load_dotenv
from typing import Tuple, Optional
import uuid
from pinecone import Pinecone
from pinecone import ServerlessSpec
from connection import getconnection, release_connection
import sqlalchemy



load_dotenv()

#! FOR INJECTION API
def insert_case(namespace: str, index_name: str, project: str):
    conn = None
    try:
        conn = getconnection()
        if not conn:
            print("Failed to connect to database")
            return False
            
        # Using SQLAlchemy connection
        sql = sqlalchemy.text("INSERT INTO volume_handling_table (namespace, index_name, project) VALUES (:namespace, :index_name, :project)")
        with conn.connect() as connection:
            connection.execute(sql, {"namespace": namespace, "index_name": index_name, "project": project})
            connection.commit()
        return True
    except Exception as e:
        if isinstance(e, pymysql.err.IntegrityError) or "Duplicate entry" in str(e):
            print(f"Duplicate namespace detected: {namespace}")
        print(e)
        return False
    finally:
        if conn:
            release_connection(conn)






def create_unique_pinecone_index(dimension: int, metric: str = "cosine", pod_type: Optional[str] = None, api_key: str = os.environ["PINECONE_API_KEY"]) -> str:
    """
    Create a new Pinecone index with a unique auto-generated name to avoid naming collisions.
    """
    pc = None
    try:
        pc = Pinecone(api_key=api_key)
        existing_indexes = pc.list_indexes()
        # Generate a unique index name
        while True:
            index_name = f"index-{uuid.uuid4().hex[:8]}"
            if index_name not in existing_indexes:
                break
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
        return index_name
    finally:
        # No need to explicitly close Pinecone client
        pass




def count_namespaces_in_index(index_name: str, api_key: str) -> int:
    """
    Count the number of namespaces in the given Pinecone index.
    """
    pc = None
    index = None
    try:
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        stats = index.describe_index_stats()
        return len(stats.get("namespaces", {}))
    finally:
        # No explicit cleanup needed for Pinecone clients
        pass




def get_index_project_by_namespace(namespace: str) -> Tuple[str, str]:
    """
    Get the index_name and project values for a given namespace from the database.
    
    Args:
        namespace (str): The namespace to look up
        
    Returns:
        Tuple[str, str]: A tuple containing (index_name, project)
        If no matching record is found, returns (None, None)
    """    
    conn = None
    try:
        conn = getconnection()
        if not conn:
            print("Failed to connect to database")
            return None, None
            
        # Using SQLAlchemy connection
        sql = sqlalchemy.text("SELECT index_name, project FROM volume_handling_table WHERE namespace = :namespace")
        with conn.connect() as connection:
            result = connection.execute(sql, {"namespace": namespace}).fetchone()
            
            if result:
                index_name = result[0]  # Access by index instead of dict
                project = result[1]
                return index_name, project
            return None, None
            
    except Exception as e:
        print(f"Error querying database: {e}")
        return None, None
    finally:
        if conn:
            release_connection(conn)



def get_index_namespace_and_project(index_name: str) -> Tuple[str, str]:
    """
    Get the namespace and project values for a given index_name from the database.
    
    Args:
        index_name (str): The index_name to look up
        
    Returns:
        Tuple[str, str]: A tuple containing (namespace, project)
        If no matching record is found, returns (None, None)
    """    
    conn = None
    try:
        conn = getconnection()
        if not conn:
            print("Failed to connect to database")
            return None, None
            
        # Using SQLAlchemy connection
        sql = sqlalchemy.text("SELECT namespace, project FROM volume_handling_table WHERE index_name = :index_name")
        with conn.connect() as connection:
            result = connection.execute(sql, {"index_name": index_name}).fetchone()
            
            if result:
                namespace = result[0]  # Access by index instead of dict
                project = result[1]
                return namespace, project
            return None, None
            
    except Exception as e:
        print(f"Error querying database: {e}")
        return None, None
    finally:
        if conn:
            release_connection(conn)

