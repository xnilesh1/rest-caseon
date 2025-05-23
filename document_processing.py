from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import os
from langchain_community.document_loaders import PyPDFLoader
from add_one_column import add_one_to_column
import requests
import os
from contextlib import contextmanager
import tempfile
from volume_handler import main_function
from pinecone_index_manager import get_index_namespace_and_project
import gc

PROJECT_1 = "QA1"
PROJECT_2 = "QA2"


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
                print(f"Warning: Failed to delete temporary file: {e}")

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
        print(f"Error loading PDF: {e}")
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
            add_one_to_column(name_space)

            embeddings = GoogleGenerativeAIEmbeddings(
                model="models/embedding-001",
                google_api_key=os.environ["GOOGLE_API_KEY"]
            )
            print(f"Using index: {index_name}")
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
                chunk_size=512,
                chunk_overlap=50,
                add_start_index=True,
            )

            # Split documents
            all_splits = text_splitter.split_documents(docs)
            
            # Add to vector store
            if all_splits:
                vector_store.add_documents(documents=all_splits)
                print(f"Processed {len(docs)} pages into {len(all_splits)} chunks")
                return f"This PDF ID is: {name_space}"
            else:
                raise ValueError("No document splits were created")

    except Exception as e:
        print(f"Error processing PDF: {e}")
        raise

    finally:
        # Explicitly clear variables to help garbage collection
        if docs:
            docs.clear()
        if all_splits:
            all_splits.clear()
            
        # Force garbage collection
        gc.collect()
