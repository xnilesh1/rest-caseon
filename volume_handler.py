from pinecone_index_manager import create_unique_pinecone_index, insert_case, count_namespaces_in_index
import os
from typing import Tuple, Dict
from pinecone import Pinecone


# Constants for Pinecone limits
NAMESPACES_PER_INDEX = 24999
INDEXES_PER_PROJECT = 20
TARGET_TOTAL_NAMESPACES = 2000000


# Project constants
PROJECT_1 = "QA1"
PROJECT_2 = "QA2"
PROJECT_3 = "QA3"
PROJECT_4 = "QA4"

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
    Returns: index_name or None if all projects are full.
    """
    # List of (env var for API key, project name)
    projects = [
        ("PINECONE_API_KEY_FIRST_PROJECT", PROJECT_1),
        ("PINECONE_API_KEY_SECOND_PROJECT", PROJECT_2),
        ("PINECONE_API_KEY_THIRD_PROJECT", PROJECT_3),
        ("PINECONE_API_KEY_FOURTH_PROJECT", PROJECT_4),
    ]
    for api_key_env, project_name in projects:
        api_key = os.environ.get(api_key_env)
        if not api_key:
            continue  # skip if API key is not set
        index_name, current_ns_count = get_project_status(api_key)
        if index_name is not None:
            insert_case(namespace=namespace_count, index_name=index_name, project=project_name)
            return index_name
    # If all projects are full, return None
    return None

