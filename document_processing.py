from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import os
from langchain_community.document_loaders import PyPDFLoader
import requests
import os
from contextlib import contextmanager
import tempfile
from volume_handler import main_function
from pinecone_index_manager import get_index_namespace_and_project
import gc
import logging
import time

PROJECT_1 = "QA1"
PROJECT_2 = "QA2"

# Configure module-level logger
logger = logging.getLogger(__name__)

# Load configuration from environment
BATCH_SIZE = int(os.getenv("PINECONE_BATCH_SIZE", 100))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 512))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", 50))
MAX_UPSERT_RETRIES = int(os.getenv("MAX_UPSERT_RETRIES", 3))
INITIAL_UPSERT_DELAY = float(os.getenv("INITIAL_UPSERT_DELAY", 1.0))

def safe_upsert(vector_store, batch):
    """
    Attempt to upsert a batch with retries and exponential backoff.
    """
    for attempt in range(MAX_UPSERT_RETRIES):
        try:
            vector_store.add_documents(documents=batch)
            return
        except Exception as e:
            logger.warning(f"Upsert attempt {attempt+1}/{MAX_UPSERT_RETRIES} failed: {e}")
            time.sleep(INITIAL_UPSERT_DELAY * (2 ** attempt))
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
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/pdf,*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        # Write to temporary file
        with open(temp_file.name, 'wb') as f:
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
            page.metadata['page'] = page.metadata['page'] + 1
        return pages
    except Exception as e:
        logger.error(f"Error loading PDF: {e}")
        raise
    finally:
        # Ensure the loader's resources are cleaned up
        if hasattr(loader, 'pdf_reader') and hasattr(loader.pdf_reader, 'stream'):
            try:
                loader.pdf_reader.stream.close()
            except:
                pass

def document_chunking_and_uploading_to_vectorstore(link, name_space):
    """
    Process PDF document with proper resource management and error handling
    """
    vector_store = None
    pc = None
    index = None
    embeddings = None
    
    try:
        index_name = main_function(name_space)
        
        # Use context manager for safe PDF download
        with safe_pdf_download(link) as pdf_path:

            embeddings = GoogleGenerativeAIEmbeddings(
                model="models/embedding-001",
                google_api_key=os.environ["GOOGLE_API_KEY"]
            )
            logger.info(f"Using index: {index_name}")
            namespace_text, project = get_index_namespace_and_project(index_name)
            
            if project == PROJECT_1:
                pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
            elif project == PROJECT_2:
                pc = Pinecone(api_key=os.environ["PINECONE_API_KEY_SECOND_PROJECT"])
            else:
                raise ValueError(f"Invalid project: {project}")
                
            index = pc.Index(index_name)
            
            vector_store = PineconeVectorStore(
                embedding=embeddings,
                index=index,
                namespace=name_space
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
            batch_size = int(os.getenv("PINECONE_BATCH_SIZE", 100))
            batch = []
            total_splits = 0
            for page in docs:
                splits = text_splitter.split_documents([page])
                total_splits += len(splits)
                for split in splits:
                    batch.append(split)
                    if len(batch) >= batch_size:
                        safe_upsert(vector_store, batch)
                        gc.collect()
                        batch.clear()
            # Flush remaining splits
            if batch:
                safe_upsert(vector_store, batch)
                gc.collect()
            # Log processing summary and return
            logger.info(f"Processed {len(docs)} pages into {total_splits} chunks ({BATCH_SIZE}-item batches)")
            return f"This PDF ID is: {name_space}"

    except Exception as e:
        logger.error(f"Error processing PDF: {e}")
        raise

    finally:
        # Explicitly clear variables to help garbage collection
        if 'docs' in locals() and docs:
            docs.clear()
        if 'batch' in locals() and batch:
            batch.clear()
        # Force garbage collection
        gc.collect()

