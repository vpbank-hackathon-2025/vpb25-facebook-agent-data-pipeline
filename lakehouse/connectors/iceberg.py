import traceback
from typing import Optional

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog

from settings.config import settings
from logs import logger


class IcebergConnector:
    """Manages Iceberg catalog connection and initialization"""
    
    def __init__(self):
        self.catalog: Optional[RestCatalog] = None
        self.namespace = settings.iceberg_namespace
        self._initialize_catalog()
    
    def _initialize_catalog(self):
        """Initialize Iceberg catalog with configuration"""
        try:
            catalog_config = {
                'uri': "http://localhost:8181",
                "type": "rest",
                # Client-side S3 configuration
                'warehouse': 's3://lakehouse/',
                "s3.region": "us-east-1",
                's3.endpoint': "http://localhost:9000",
                's3.access-key-id': "minioadmin",
                's3.secret-access-key': "minioadmin",
                's3.path-style-access': 'true',
                's3.signer-type': 'S3SignerType',
                's3.disable-bucket-location-inference': 'true'
            }
            
            self.catalog = load_catalog(self.namespace, **catalog_config)
            logger.info("Iceberg catalog initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {e}")
            traceback.print_exc()
            raise Exception(f"Catalog initialization failed: {e}")
    
    def get_catalog(self) -> RestCatalog:
        """Get catalog instance"""
        if self.catalog is None:
            raise Exception("Catalog not initialized")
        return self.catalog
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in catalog"""
        try:
            self.catalog.load_table(f"{self.namespace}.{table_name}")
            return True
        except:
            return False


# Singleton instance
iceberg_connector = IcebergConnector()