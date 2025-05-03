from pinecone_index_manager import create_unique_pinecone_index, insert_case, count_namespaces_in_index
import os
from typing import Tuple, Dict
from pinecone import Pinecone

# Constants for Pinecone limits
# NAMESPACES_PER_INDEX = 25000
NAMESPACES_PER_INDEX = 1
INDEXES_PER_PROJECT = 20
TARGET_TOTAL_NAMESPACES = 1000000

# Project constants
PROJECT_1 = "QA1"
PROJECT_2 = "QA2"

def get_project_status(api_key: str) -> Tuple[str, int]:
    """Get current project status including available index and total namespaces."""
    # Initialize Pinecone
    pc = Pinecone(api_key=api_key)
    
    # Get list of existing indexes
    indexes = pc.list_indexes()
    
    # If no indexes exist, create first one
    if not indexes:
        return create_unique_pinecone_index(dimension=768, metric="cosine", api_key=api_key), 0
    
    # Check each existing index for available capacity
    for index in indexes:
        ns_count = count_namespaces_in_index(index_name=index.name, api_key=api_key)
        # ns_count = 25000
        if ns_count < NAMESPACES_PER_INDEX:
            return index.name, ns_count
    
    # If all indexes are full and we haven't reached project limit
    if len(indexes) < INDEXES_PER_PROJECT:
        return create_unique_pinecone_index(dimension=768, metric="cosine", api_key=api_key), 0
    
    return None, None

def main_function(namespace_count: str) -> str:
    """
    Main handler for managing Pinecone indexes and namespaces across projects.
    Returns: index_name
    """
    # Try first project
    api_key = os.environ["PINECONE_API_KEY"]
    index_name, current_ns_count = get_project_status(api_key)
    
    if index_name is not None:
        # If first project has capacity, use it
        insert_case(namespace=namespace_count, index_name=index_name, project=PROJECT_1)
        return index_name
    
    # If first project is full, try second project
    api_key = os.environ["PINECONE_API_KEY_SECOND_PROJECT"]
    index_name, current_ns_count = get_project_status(api_key)
    
    if index_name is None:
        raise Exception("Both projects are at capacity. Cannot create more indexes.")
    
    # Insert into second project
    insert_case(namespace=namespace_count, index_name=index_name, project=PROJECT_2)
    return index_name

