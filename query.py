import logging
import os
from pinecone import Pinecone
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from pydantic import BaseModel
from typing import List, Dict, Tuple
from dotenv import load_dotenv
from pinecone_index_manager import get_index_project_by_namespace
import gc
from pdf_daily_tracker import track_pdf_daily_usage

load_dotenv()
class PineconeVectorStore(BaseModel):
    index_name: str
    query: str

class QueryResult(BaseModel):
    text: str
    metadata: Dict
    score: float


PROJECT_1 = "QA1"
PROJECT_2 = "QA2"
PROJECT_3 = "QA3"
PROJECT_4 = "QA4"


def pincone_vector_database_query(query: str, namespace: str):
    pc = None
    index = None
    try:
        track_pdf_daily_usage(namespace)
        logging.info(f"Tracked Today's usage of {namespace}")
        """
        Query the Pinecone vector database and return results with full metadata
        
        Args:
            query (str): The query text
            index_name (str): Name of the Pinecone index
        
        Returns:
            Tuple[List[str], List[Dict]]: Returns (texts, metadata_list)
        """

        # Initialize embeddings and Pinecone
        print(f"Getting index and project for namespace: {namespace}")
        index_name, project = get_index_project_by_namespace(namespace)
        print(f"Retrieved index_name: {index_name}, project: {project}")
        
        if not index_name or not project:
            raise ValueError(f"No index or project found for namespace: {namespace}")
        
        embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001", google_api_key=os.environ["GOOGLE_API_KEY"])
        print(f"Using project: {project}")
        if project == PROJECT_1:
            pc = Pinecone(api_key=os.environ["PINECONE_API_KEY_FIRST_PROJECT"])
            index = pc.Index(index_name)
        elif project == PROJECT_2:
            pc = Pinecone(api_key=os.environ["PINECONE_API_KEY_SECOND_PROJECT"])
            index = pc.Index(index_name)
        elif project == PROJECT_3:
            pc = Pinecone(api_key=os.environ["PINECONE_API_KEY_THIRD_PROJECT"])
            index = pc.Index(index_name)
        elif project == PROJECT_4:
            pc = Pinecone(api_key=os.environ["PINECONE_API_KEY_FOURTH_PROJECT"])
            index = pc.Index(index_name)
        else:
            raise ValueError(f"Invalid project: {project}")
        
        # Get query embedding
        query_embedding = embeddings.embed_query(query)
        
        # Query Pinecone
        results = index.query(
            vector=query_embedding,
            top_k=30,
            include_metadata=True,
            namespace=namespace,
        )
        
        # Extract results and metadata
        query_results = []
        for match in results["matches"]:
            text = match["metadata"].get("text", "")
            metadata = {
                "page": match["metadata"].get("page", "Unknown"),
                "score": match["score"],
                # Add any other metadata fields you want to track
                "chunk_index": match["metadata"].get("chunk_index", "Unknown"),
            }
            query_results.append(QueryResult(text=text, metadata=metadata, score=match["score"]))
        
        # Return both texts and full metadata
        texts = [result.text for result in query_results]
        metadata_list = [result.metadata for result in query_results]
        return texts, metadata_list
        
    except Exception as e:
        print(f"An error occurred in pinecone vector database query: {str(e)}")
        import traceback
        print("Full error details:")
        print(traceback.format_exc())
        return None, None
    
    finally:
        # Help garbage collection by clearing references and forcing collection
        embeddings = None
        query_embedding = None
        results = None
        query_results = None
        
        # No explicit cleanup needed for Pinecone clients
        pc = None
        index = None
        
        # Force garbage collection
        gc.collect()
    
