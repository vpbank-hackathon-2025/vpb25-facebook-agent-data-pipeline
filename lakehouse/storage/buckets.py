"""Bucket management operations"""
from typing import List
from minio.error import S3Error

from lakehouse.connectors.s3 import s3_connector
from settings.config import settings
from logs import logger


class BucketManager:
    """Manages S3/MinIO bucket operations"""
    
    def __init__(self):
        self.client = s3_connector.get_client()
        self._ensure_required_buckets()
    
    def _ensure_required_buckets(self):
        """Ensure all required buckets exist"""
        required_buckets = [
            settings.staging_bucket,
            settings.silver_bucket,
            settings.lakehouse_bucket
        ]
        
        for bucket in required_buckets:
            try:
                if not self.bucket_exists(bucket):
                    self.create_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                logger.error(f"Error ensuring bucket {bucket}: {e}")
                raise Exception(f"Failed to ensure bucket {bucket}: {e}")
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if bucket exists"""
        try:
            return self.client.bucket_exists(bucket_name)
        except S3Error as e:
            logger.error(f"Error checking bucket existence {bucket_name}: {e}")
            raise Exception(f"Failed to check bucket {bucket_name}: {e}")
    
    def create_bucket(self, bucket_name: str) -> bool:
        """Create a new bucket"""
        try:
            self.client.make_bucket(bucket_name)
            logger.info(f"Successfully created bucket: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            raise Exception(f"Failed to create bucket {bucket_name}: {e}")
    
    def delete_bucket(self, bucket_name: str) -> bool:
        """Delete a bucket (must be empty)"""
        try:
            self.client.remove_bucket(bucket_name)
            logger.info(f"Successfully deleted bucket: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting bucket {bucket_name}: {e}")
            raise Exception(f"Failed to delete bucket {bucket_name}: {e}")
    
    def list_buckets(self) -> List[str]:
        """List all buckets"""
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logger.error(f"Error listing buckets: {e}")
            raise Exception(f"Failed to list buckets: {e}")


# Singleton instance
bucket_manager = BucketManager()