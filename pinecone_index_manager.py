import os
import pymysql
from dotenv import load_dotenv
from typing import List, Dict, Tuple, Optional
import uuid
from pinecone import Pinecone
from pinecone import ServerlessSpec
from connection import getconnection, release_connection



load_dotenv()

#! FOR INJECTION API



def insert_case(namespace: str, index_name: str, project: str):
    conn = None
    try:
        conn = getconnection()
        if not conn:
            print("Failed to connect to database")
            return False
            
        with conn.cursor() as cursor:
            sql = "INSERT INTO volume_handling_table (namespace, index_name, project) VALUES (%s, %s, %s)"
            cursor.execute(sql, (namespace, index_name, project))
        conn.commit()
        return True
    except pymysql.err.IntegrityError as e:
        if "Duplicate entry" in str(e) and "PRIMARY" in str(e):
            print(f"Duplicate namespace detected: {namespace}")
        print(e)
        return False
    except Exception as e:
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


# if __name__ == "__main__":
#     # create_case_details_table()
#     insert_case(index_name="index-123", namespace="namespace-123", project="QA1")


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
            
        with conn.cursor() as cursor:
            sql = "SELECT index_name, project FROM volume_handling_table WHERE namespace = %s"
            cursor.execute(sql, (namespace,))
            result = cursor.fetchone()
            
            if result:
                index_name = result['index_name']
                project = result['project']
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
            
        with conn.cursor() as cursor:
            sql = "SELECT namespace, project FROM volume_handling_table WHERE index_name = %s"
            cursor.execute(sql, (index_name,))
            result = cursor.fetchone()
            
            if result:
                namespace = result['namespace']
                project = result['project']
                return namespace, project
            return None, None
            
    except Exception as e:
        print(f"Error querying database: {e}")
        return None, None
    finally:
        if conn:
            release_connection(conn)

