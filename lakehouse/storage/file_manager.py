import io
from typing import List, Dict, Any, Optional
from datetime import datetime
from minio.error import S3Error

from lakehouse.connectors.s3 import s3_connector
from logs import logger


class FileManager:
    """Manages file operations in S3/MinIO"""
    
    def __init__(self):
        self.client = s3_connector.get_client()
    
    def upload_file(self, bucket: str, object_name: str, file_data: bytes, 
                   content_type: str = "application/octet-stream",
                   metadata: Optional[Dict[str, str]] = None) -> bool:
        """Upload file to MinIO"""
        try:
            file_stream = io.BytesIO(file_data)
            
            self.client.put_object(
                bucket,
                object_name,
                file_stream,
                length=len(file_data),
                content_type=content_type,
                metadata=metadata or {}
            )
            
            logger.info(f"Successfully uploaded {object_name} to {bucket}")
            return True
            
        except S3Error as e:
            logger.error(f"Error uploading file {object_name}: {e}")
            raise Exception(f"Failed to upload {object_name}: {e}")
    
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
            raise Exception(f"Failed to download {object_name}: {e}")
    
    def list_files(self, bucket: str, prefix: str = "", 
                  include_metadata: bool = False) -> List[Dict[str, Any]]:
        """List files in bucket with optional prefix"""
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            
            if include_metadata:
                return [
                    {
                        "name": obj.object_name,
                        "size": obj.size,
                        "last_modified": obj.last_modified,
                        "etag": obj.etag,
                        "is_dir": obj.is_dir
                    }
                    for obj in objects
                ]
            else:
                return [{"name": obj.object_name} for obj in objects]
                
        except S3Error as e:
            logger.error(f"Error listing files in bucket {bucket}: {e}")
            raise Exception(f"Failed to list files in {bucket}: {e}")
    
    def delete_file(self, bucket: str, object_name: str) -> bool:
        """Delete file from MinIO"""
        try:
            self.client.remove_object(bucket, object_name)
            logger.info(f"Successfully deleted {object_name} from {bucket}")
            return True
            
        except S3Error as e:
            logger.error(f"Error deleting file {object_name}: {e}")
            raise Exception(f"Failed to delete {object_name}: {e}")
    
    def move_file(self, source_bucket: str, source_object: str,
                 dest_bucket: str, dest_object: str) -> bool:
        """Move file between buckets/locations"""
        try:
            # Copy then delete original
            self.copy_file(source_bucket, source_object, dest_bucket, dest_object)
            self.delete_file(source_bucket, source_object)
            
            logger.info(f"Successfully moved {source_object} to {dest_object}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving file: {e}")
            raise Exception(f"Failed to move file: {e}")
    
    def copy_file(self, source_bucket: str, source_object: str,
                 dest_bucket: str, dest_object: str) -> bool:
        """Copy file between buckets/locations"""
        try:
            from minio.commonconfig import CopySource
            
            self.client.copy_object(
                dest_bucket,
                dest_object,
                CopySource(source_bucket, source_object)
            )
            
            logger.info(f"Successfully copied {source_object} to {dest_object}")
            return True
            
        except S3Error as e:
            logger.error(f"Error copying file: {e}")
            raise Exception(f"Failed to copy file: {e}")
    
    def file_exists(self, bucket: str, object_name: str) -> bool:
        """Check if file exists in bucket"""
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False
    
    def get_file_info(self, bucket: str, object_name: str) -> Dict[str, Any]:
        """Get file metadata"""
        try:
            stat = self.client.stat_object(bucket, object_name)
            return {
                "name": object_name,
                "size": stat.size,
                "last_modified": stat.last_modified,
                "etag": stat.etag,
                "content_type": stat.content_type,
                "metadata": stat.metadata or {}
            }
            
        except S3Error as e:
            logger.error(f"Error getting file info for {object_name}: {e}")
            raise Exception(f"Failed to get info for {object_name}: {e}")


# Singleton instance
file_manager = FileManager()