import io
from typing import List, Tuple
from minio import Minio
from minio.error import S3Error

from core.config import settings
from logs import logger


class MinIOService:
    def __init__(self):
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure
        )
        self._ensure_buckets()
    
    def _ensure_buckets(self):
        """Ensure all required buckets exist"""
        buckets = [
            settings.staging_bucket,
            settings.silver_bucket,
            settings.lakehouse_bucket
        ]
        
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
    
    def upload_file(self, bucket: str, object_name: str, file_data: bytes, 
                   content_type: str = "application/octet-stream") -> bool:
        """Upload file to MinIO"""
        try:
            file_stream = io.BytesIO(file_data)
            self.client.put_object(
                bucket,
                object_name,
                file_stream,
                length=len(file_data),
                content_type=content_type
            )
            logger.info(f"Successfully uploaded {object_name} to {bucket}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading file {object_name}: {e}")
            return False
    
    def download_file(self, bucket: str, object_name: str) -> bytes:
        """Download file from MinIO"""
        try:
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Error downloading file {object_name}: {e}")
            raise
    
    def list_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List files in bucket with optional prefix"""
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing files in bucket {bucket}: {e}")
            return []
    
    def delete_file(self, bucket: str, object_name: str) -> bool:
        """Delete file from MinIO"""
        try:
            self.client.remove_object(bucket, object_name)
            logger.info(f"Successfully deleted {object_name} from {bucket}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting file {object_name}: {e}")
            return False
    
    def get_file_info(self, bucket: str, object_name: str) -> dict:
        """Get file metadata"""
        try:
            stat = self.client.stat_object(bucket, object_name)
            return {
                "size": stat.size,
                "last_modified": stat.last_modified,
                "etag": stat.etag,
                "content_type": stat.content_type
            }
        except S3Error as e:
            logger.error(f"Error getting file info for {object_name}: {e}")
            return {}
    
    def file_exists(self, bucket: str, object_name: str) -> bool:
        """Check if file exists in bucket"""
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False


# Singleton instance
minio_service = MinIOService()