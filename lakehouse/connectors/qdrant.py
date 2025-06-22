from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import uuid
from typing import List, Dict, Any
from datetime import datetime

from logs import logger

class QdrantConnector:
    def __init__(self, qdrant_url: str, qdrant_api_key: str = None, collection_name: str = "agent_documents"):
        """
        Initialize Qdrant connection
        
        Args:
            qdrant_url: Qdrant server URL
            qdrant_api_key: Qdrant API key (optional for local)
            collection_name: Name of the collection to use
        """
        self.qdrant_client = QdrantClient(
            url=qdrant_url,
            api_key=qdrant_api_key
        )
        self.collection_name = collection_name
        
        # Initialize collection if not exists
        self._create_collection_if_not_exists()
    
    def _create_collection_if_not_exists(self, vector_size: int = 768):
        """
        Tạo collection trong Qdrant nếu chưa tồn tại
        
        Args:
            vector_size: Dimension của vector (default 768 cho Gemini)
        """
        try:
            collections = self.qdrant_client.get_collections()
            collection_names = [c.name for c in collections.collections]
            
            if self.collection_name not in collection_names:
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=vector_size,
                        distance=Distance.COSINE
                    )
                )
                logger.info(f"Created collection: {self.collection_name}")
            else:
                logger.info(f"Collection {self.collection_name} already exists")
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            raise
    
    def upload_points(self, chunks: List[str], embeddings: List[List[float]], 
                     metadata: Dict[str, Any]) -> bool:
        """
        Upload chunks và embeddings lên Qdrant
        
        Args:
            chunks: List of text chunks
            embeddings: List of embedding vectors
            metadata: Additional metadata for all chunks
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            points = []
            
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                if embedding is None:
                    logger.warning(f"Skipping chunk {i} due to missing embedding")
                    continue
                    
                point_id = str(uuid.uuid4())
                
                payload = {
                    "text": chunk,
                    "chunk_index": i,
                    "source_file": metadata.get("source_file", ""),
                    "upload_timestamp": datetime.now().isoformat(),
                    "text_length": len(chunk),
                    **metadata
                }
                
                point = PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=payload
                )
                points.append(point)
            
            if points:
                self.qdrant_client.upsert(
                    collection_name=self.collection_name,
                    points=points
                )
                logger.info(f"Successfully uploaded {len(points)} points to Qdrant collection '{self.collection_name}'")
                return True
            else:
                logger.warning("No valid points to upload")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading to Qdrant: {e}")
            return False
    
    def search(self, query_vector: List[float], limit: int = 10, 
               score_threshold: float = 0.0, filter_conditions: Dict = None) -> List[Dict]:
        """
        Search for similar vectors in Qdrant
        
        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results
            score_threshold: Minimum similarity score
            filter_conditions: Qdrant filter conditions
            
        Returns:
            List of search results with scores and payloads
        """
        try:
            search_result = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                score_threshold=score_threshold,
                query_filter=filter_conditions
            )
            
            results = []
            for scored_point in search_result:
                results.append({
                    "id": scored_point.id,
                    "score": scored_point.score,
                    "payload": scored_point.payload
                })
            
            logger.info(f"Found {len(results)} similar documents")
            return results
            
        except Exception as e:
            logger.error(f"Error searching in Qdrant: {e}")
            return []
    
    def delete_by_filter(self, filter_conditions: Dict) -> bool:
        """
        Delete points by filter conditions
        
        Args:
            filter_conditions: Qdrant filter conditions
            
        Returns:
            bool: True if successful
        """
        try:
            self.qdrant_client.delete(
                collection_name=self.collection_name,
                points_selector=filter_conditions
            )
            logger.info("Successfully deleted points by filter")
            return True
        except Exception as e:
            logger.error(f"Error deleting points: {e}")
            return False
    
    def get_collection_info(self) -> Dict:
        """
        Get information about the collection
        
        Returns:
            Dictionary with collection information
        """
        try:
            info = self.qdrant_client.get_collection(self.collection_name)
            return {
                "name": info.config.name,
                "status": info.status,
                "vectors_count": info.vectors_count,
                "points_count": info.points_count,
                "vector_size": info.config.params.vectors.size,
                "distance": info.config.params.vectors.distance
            }
        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            return {}
    
    def delete_collection(self) -> bool:
        """
        Delete the entire collection
        
        Returns:
            bool: True if successful
        """
        try:
            self.qdrant_client.delete_collection(self.collection_name)
            logger.info(f"Successfully deleted collection '{self.collection_name}'")
            return True
        except Exception as e:
            logger.error(f"Error deleting collection: {e}")
            return False