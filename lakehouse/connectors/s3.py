from minio import Minio
from minio.error import S3Error

from settings.config import settings
from logs import logger


class S3Connector:
    """Manages MinIO/S3 connection"""
    
    def __init__(self):
        self._client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize MinIO client"""
        try:
            self._client = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure
            )
            logger.info("MinIO client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise Exception(f"MinIO client initialization failed: {e}")
    
    def get_client(self) -> Minio:
        """Get MinIO client instance"""
        if self._client is None:
            raise Exception("MinIO client not initialized")
        return self._client
    
    def test_connection(self) -> bool:
        """Test MinIO connection"""
        try:
            # Try to list buckets to test connection
            list(self._client.list_buckets())
            return True
        except S3Error as e:
            logger.error(f"MinIO connection test failed: {e}")
            return False


# Singleton instance
s3_connector = S3Connector()