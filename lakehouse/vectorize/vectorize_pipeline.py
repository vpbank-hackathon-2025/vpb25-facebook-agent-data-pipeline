from google import genai
import os
import time
from typing import List, Dict, Any
from datetime import datetime

from logs import logger
from lakehouse.connectors.qdrant import QdrantConnector
from lakehouse.connectors.s3 import s3_connector

class S3ToQdrantPipeline:
    def __init__(self, 
                 aws_access_key: str,
                 aws_secret_key: str, 
                 bucket_name: str,
                 gemini_api_key: str,
                 qdrant_url: str,
                 qdrant_api_key: str = None,
                 collection_name: str = "documents"):
        
        # S3 setup
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        self.bucket_name = bucket_name
        
        # Gemini setup
        genai.configure(api_key=gemini_api_key)
        
        # Qdrant setup
        self.qdrant_connector = QdrantConnector(
            qdrant_url=qdrant_url,
            qdrant_api_key=qdrant_api_key,
            collection_name=collection_name
        )
    
    def read_file_from_s3(self, s3_key: str) -> str:
        """Đọc file từ S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read()
            
            # Detect encoding and decode
            try:
                text = content.decode('utf-8')
            except UnicodeDecodeError:
                text = content.decode('utf-8', errors='ignore')
            
            logger.info(f"Successfully read file: {s3_key}")
            return text
        
        except Exception as e:
            logger.error(f"Error reading file {s3_key}: {e}")
            return ""
    
    def chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        """Chia văn bản thành các chunks"""
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            # Tìm điểm ngắt tự nhiên (dấu câu, xuống dòng)
            if end < len(text):
                # Tìm ngược lại để tìm dấu câu hoặc xuống dòng
                for i in range(end, max(start + chunk_size // 2, start), -1):
                    if text[i] in '.!?\n':
                        end = i + 1
                        break
            
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            start = end - overlap
            if start >= len(text):
                break
        
        return chunks
    
    def get_gemini_embedding(self, text: str) -> List[float]:
        """Tạo embedding sử dụng Gemini"""
        try:
            # Sử dụng text-embedding-004 model
            result = genai.embed_content(
                model="models/text-embedding-004",
                content=text,
                task_type="retrieval_document"
            )
            return result['embedding']
        
        except Exception as e:
            logger.error(f"Error getting embedding: {e}")
            # Retry sau 1 giây
            time.sleep(1)
            try:
                result = genai.embed_content(
                    model="models/text-embedding-004",
                    content=text,
                    task_type="retrieval_document"
                )
                return result['embedding']
            except Exception as e2:
                logger.error(f"Error getting embedding (retry): {e2}")
                return None
    
    def upload_to_qdrant(self, chunks: List[str], embeddings: List[List[float]], 
                        metadata: Dict[str, Any]) -> bool:
        """Upload chunks và embeddings lên Qdrant"""
        return self.qdrant_connector.upload_points(chunks, embeddings, metadata)
    
    def process_file(self, s3_key: str, chunk_size: int = 1000, overlap: int = 200) -> bool:
        """Xử lý một file từ S3"""
        logger.info(f"Processing file: {s3_key}")
        
        # 1. Đọc file từ S3
        content = self.read_file_from_s3(s3_key)
        if not content:
            return False
        
        # 2. Chunking
        chunks = self.chunk_text(content, chunk_size, overlap)
        logger.info(f"Created {len(chunks)} chunks")
        
        # 3. Tạo embeddings
        embeddings = []
        for i, chunk in enumerate(chunks):
            logger.info(f"Creating embedding for chunk {i+1}/{len(chunks)}")
            embedding = self.get_gemini_embedding(chunk)
            embeddings.append(embedding)
            
            # Rate limiting
            time.sleep(0.1)
        
        # 4. Upload lên Qdrant
        metadata = {
            "source_file": s3_key,
            "total_chunks": len(chunks),
            "chunk_size": chunk_size,
            "overlap": overlap
        }
        
        success = self.upload_to_qdrant(chunks, embeddings, metadata)
        return success
    
    def process_files(self, s3_keys: List[str], chunk_size: int = 1000, overlap: int = 200):
        """Xử lý danh sách files"""
        logger.info(f"Starting to process {len(s3_keys)} files")
        
        successful = 0
        failed = 0
        
        for i, s3_key in enumerate(s3_keys):
            logger.info(f"Processing file {i+1}/{len(s3_keys)}: {s3_key}")
            
            try:
                success = self.process_file(s3_key, chunk_size, overlap)
                if success:
                    successful += 1
                    logger.info(f"✅ Successfully processed: {s3_key}")
                else:
                    failed += 1
                    logger.error(f"❌ Failed to process: {s3_key}")
                    
            except Exception as e:
                failed += 1
                logger.error(f"❌ Error processing {s3_key}: {e}")
            
            # Rate limiting giữa các files
            time.sleep(1)
        
        logger.info(f"Processing completed. Successful: {successful}, Failed: {failed}")
    
    def search_similar(self, query_text: str, limit: int = 10) -> List[Dict]:
        """Tìm kiếm documents tương tự"""
        # Tạo embedding cho query
        query_embedding = self.get_gemini_embedding(query_text)
        if query_embedding is None:
            logger.error("Failed to create embedding for query")
            return []
        
        # Search trong Qdrant
        return self.qdrant_connector.search(query_embedding, limit=limit)
    
    def get_collection_info(self) -> Dict:
        """Lấy thông tin collection"""
        return self.qdrant_connector.get_collection_info()
    
    def delete_collection(self) -> bool:
        """Xóa collection"""
        return self.qdrant_connector.delete_collection()