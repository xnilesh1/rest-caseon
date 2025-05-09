import gc
import json
import logging
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from typing import Dict, Optional

import requests
from langchain_community.document_loaders import PyPDFLoader
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pinecone import Pinecone
from pinecone_index_manager import get_index_namespace_and_project
from volume_handler import main_function

PROJECT_1 = "QA1"
PROJECT_2 = "QA2"
PROJECT_3 = "QA3"
PROJECT_4 = "QA4"


# Configure module-level logger
logger = logging.getLogger(__name__)

# Load configuration from environment
BATCH_SIZE = 100
CHUNK_SIZE = 512
CHUNK_OVERLAP = 50
MAX_UPSERT_RETRIES = 3
INITIAL_UPSERT_DELAY = 1.0


def safe_upsert(vector_store, batch):
    """
    Attempt to upsert a batch with retries and exponential backoff.
    """
    for attempt in range(MAX_UPSERT_RETRIES):
        try:
            vector_store.add_documents(documents=batch)
            return
        except Exception as e:
            logger.warning(
                f"Upsert attempt {attempt+1}/{MAX_UPSERT_RETRIES} failed: {e}"
            )
            time.sleep(INITIAL_UPSERT_DELAY * (2**attempt))
    raise RuntimeError("Failed to upsert batch after retries")


@contextmanager
def safe_pdf_download(url):
    """
    Context manager for safely downloading and cleaning up PDF files.
    """
    temp_file = None
    response = None
    try:
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/pdf,*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        # Write to temporary file
        with open(temp_file.name, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        yield temp_file.name

    finally:
        # Clean up resources
        if response:
            response.close()
        if temp_file:
            temp_file.close()
            try:
                if os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file: {e}")


def process_pdf_safely(loader):
    """
    Safely process PDF pages with proper resource management
    """
    try:
        pages = loader.load()
        # Add page numbers to metadata
        for page in pages:
            page.metadata["page"] = page.metadata["page"] + 1
        return pages
    except Exception as e:
        logger.error(f"Error loading PDF: {e}")
        raise
    finally:
        # Ensure the loader's resources are cleaned up
        if hasattr(loader, "pdf_reader") and hasattr(loader.pdf_reader, "stream"):
            try:
                loader.pdf_reader.stream.close()
            except Exception as e:
                logger.warning(f"Error closing PDF reader stream: {e}")


def pdf_processor_subprocess(pdf_path: str, params: Dict, result_file: str):
    """
    Process PDF in a separate subprocess and save results to temp file.
    This function runs in a completely separate process.
    """
    try:
        # Extract parameters
        name_space = params['name_space']
        index_name = params['index_name']
        project = params['project']
        api_key = params['api_key']
        
        # Set up embeddings and vector store
        embeddings = GoogleGenerativeAIEmbeddings(
            model="models/embedding-001",
            google_api_key=os.environ["GOOGLE_API_KEY"],
        )
        
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        
        vector_store = PineconeVectorStore(
            embedding=embeddings, index=index, namespace=name_space
        )
        
        # Load and process PDF
        loader = PyPDFLoader(file_path=pdf_path)
        docs = process_pdf_safely(loader)
        
        # Configure text splitter
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
            add_start_index=True,
        )
        
        # Batch-split and upsert documents for memory efficiency
        batch_size = int(os.getenv("PINECONE_BATCH_SIZE", str(BATCH_SIZE)))
        batch = []
        total_splits = 0
        
        for page in docs:
            splits = text_splitter.split_documents([page])
            total_splits += len(splits)
            for split in splits:
                batch.append(split)
                if len(batch) >= batch_size:
                    safe_upsert(vector_store, batch)
                    batch = []
                    gc.collect()
        
        # Flush remaining splits
        if batch:
            safe_upsert(vector_store, batch)
            batch = []
            gc.collect()
        
        # Write success result to temp file
        with open(result_file, 'w') as f:
            json.dump({"success": True, "total_splits": total_splits, "pages": len(docs)}, f)
            
    except Exception as e:
        # Write error to temp file
        with open(result_file, 'w') as f:
            json.dump({"success": False, "error": str(e)}, f)
        logger.error(f"Subprocess error processing PDF: {e}")


def document_chunking_and_uploading_to_vectorstore(link, name_space):
    """
    Process PDF document using a subprocess for memory isolation
    """
    try:
        # Get index name for this namespace
        index_name = main_function(name_space)
        
        # Get project info to determine which API key to use
        namespace_text, project = get_index_namespace_and_project(index_name)
        
        # Select API key based on project
        if project == PROJECT_1:
            api_key = os.environ["PINECONE_API_KEY_FIRST_PROJECT"]
        elif project == PROJECT_2:
            api_key = os.environ["PINECONE_API_KEY_SECOND_PROJECT"]
        elif project == PROJECT_3:
            api_key = os.environ["PINECONE_API_KEY_THIRD_PROJECT"]
        elif project == PROJECT_4:
            api_key = os.environ["PINECONE_API_KEY_FOURTH_PROJECT"]
        else:
            raise ValueError(f"Invalid project: {project}")
            
        # Use context manager for safe PDF download
        with safe_pdf_download(link) as pdf_path:
            # Create temporary file for subprocess results
            with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as result_file:
                result_path = result_file.name
            
            # Parameters to pass to the subprocess
            params = {
                'name_space': name_space,
                'index_name': index_name,
                'project': project,
                'api_key': api_key
            }
            
            # Start the subprocess
            process = multiprocessing.Process(
                target=pdf_processor_subprocess,
                args=(pdf_path, params, result_path)
            )
            
            logger.info(f"Starting PDF processing subprocess for namespace: {name_space}")
            process.start()
            process.join()  # Wait for process to complete
            
            # Check the results from the subprocess
            with open(result_path, 'r') as f:
                result = json.load(f)
            
            # Clean up the temporary result file
            try:
                os.unlink(result_path)
            except Exception as e:
                logger.warning(f"Failed to delete temporary result file: {e}")
            
            if not result.get('success', False):
                raise RuntimeError(f"Subprocess failed: {result.get('error', 'Unknown error')}")
            
            # Log processing summary
            logger.info(
                f"Processed {result.get('pages', 0)} pages into {result.get('total_splits', 0)} "
                f"chunks ({BATCH_SIZE}-item batches)"
            )
            
            return f"This PDF ID is: {name_space}"

    except Exception as e:
        logger.error(f"Error processing PDF: {e}")
        raise
