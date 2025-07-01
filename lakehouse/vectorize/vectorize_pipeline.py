import os
import time
import sys
import traceback

from typing import List
from minio.error import S3Error

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from logs import logger
from lakehouse.connectors.qdrant import qdrant_connector
from lakehouse.connectors.s3 import s3_connector
from settings.config import settings
from ai_services.embedding import GeminiEmbeddingService


class TextProcessor:
    @staticmethod
    def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            if end >= len(text):
                chunk = text[start:]
            else:
                # Try to break at sentence boundary
                boundary = text.rfind('.', start, end)
                if boundary == -1 or boundary <= start:
                    boundary = text.rfind(' ', start, end)
                    if boundary == -1 or boundary <= start:
                        boundary = end
                chunk = text[start:boundary + 1]
            
            chunks.append(chunk.strip())
            start = max(start + chunk_size - overlap, boundary + 1)
            
        return chunks


class VectorizePipeline:
    def __init__(self):
        self.s3_client = s3_connector
        self.qdrant_connector = qdrant_connector
        self.embedding_service = GeminiEmbeddingService()
        self.text_processor = TextProcessor()
        
    def read_file_from_s3(self, bucket_name: str, file_key: str) -> str | None:
        try:
            client = self.s3_client.get_client()
            response = client.get_object(bucket_name, file_key)
            content = response.read().decode('utf-8')
            return content
        except S3Error as e:
            logger.error(f"Error reading file {file_key} from S3: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error reading file {file_key}: {e}")
            return None
    
    def process_file_list(self, file_paths: List[str], 
                         chunk_size: int = 1000, overlap: int = 200) -> bool:
        successful_uploads = 0
        
        for raw_file_path in file_paths:
            # bucket name is the first part of the path
            bucket_name = raw_file_path.split('/')[0]
            # Remove bucket name
            file_path = '/'.join(raw_file_path.split('/')[1:])
            
            try:
                logger.info(f"Vectorize processing file: {file_path}")
                
                # Read file content
                content = self.read_file_from_s3(bucket_name, file_path)
                if not content:
                    logger.warning(f"Skipping empty or unreadable file: {file_path}")
                    continue
                
                # Chunk text
                chunks = self.text_processor.chunk_text(content, chunk_size, overlap)
                if not chunks:
                    logger.warning(f"No chunks generated for file: {file_path}")
                    continue
                # Log chunking details
                logger.info(f"Generated {len(chunks)} chunks for {file_path}")
                
                # Get embeddings for all chunks
                embeddings = self.embedding_service.get_embeddings_batch(chunks)
                logger.info(f"Generated embeddings for {len(embeddings)} chunks")
                
                # Upload to Qdrant
                success = self.qdrant_connector.upload_points(
                    chunks=chunks,
                    embeddings=embeddings,
                    metadata={
                        "source_file": raw_file_path
                    }
                )

                if success:
                    successful_uploads += 1
                    logger.info(f"Successfully processed and uploaded {file_path}")
                else:
                    logger.error(f"Failed to upload vectors for {file_path}")
                    
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                logger.error(traceback.format_exc())
                continue
        return successful_uploads > 0
    
    def process_summary_details_file(self, summary_details: str, s3_file_content: str, extracted_content: str,
                                     chunk_size: int = 1000, overlap: int = 200) -> bool:
        try:
            # Chunk the summary details
            chunks = self.text_processor.chunk_text(summary_details, chunk_size, overlap)
            if not chunks:
                logger.warning("No chunks generated from summary details.")
                return False
            
            # Get embeddings for all chunks
            embeddings = self.embedding_service.get_embeddings_batch(chunks)
            logger.info(f"Generated embeddings for {len(embeddings)} summary chunks")
            
            # Upload to Qdrant
            success = self.qdrant_connector.upload_points(
                chunks=chunks,
                embeddings=embeddings,
                metadata={
                    "source": s3_file_content,
                    "extracted_content": extracted_content.get("text", ""),
                }
            )
            
            if success:
                logger.info("Successfully processed and uploaded summary details.")
                return True
            else:
                logger.error("Failed to upload vectors for summary details.")
                return False
        except Exception as e:
            logger.error(f"Error processing summary details: {e}")
            logger.error(traceback.format_exc())
            return False

if __name__ == "__main__":
    # Example usage
    file_paths = [
        "silver/pdfs/tnc-shopee-032023.md"
    ]
    
    pipeline = VectorizePipeline()
    success = pipeline.process_file_list(file_paths)
    if success:
        logger.info("Vectorization completed successfully.")
    else:
        logger.error("Vectorization failed.")
