import traceback
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa

from lakehouse.connectors.iceberg import iceberg_connector
from lakehouse.storage.schemas import TableSchemas
from logs import logger
from api.models.schemas import PDFDocument, TXTDocument


class IcebergTableManager:
    """Manages Iceberg table operations"""
    
    def __init__(self):
        self.catalog = iceberg_connector.get_catalog()
        self.namespace = iceberg_connector.namespace
        self.schemas = TableSchemas()
        self._ensure_tables()
    
    def _ensure_tables(self):
        """Ensure all required tables exist"""
        try:
            if not iceberg_connector.table_exists("pdf_documents"):
                self._create_pdf_table()
            
            if not iceberg_connector.table_exists("txt_documents"):
                self._create_txt_table()
                
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {e}")
            traceback.print_exc()
            raise Exception(f"Failed to ensure tables: {e}")
    
    def _create_pdf_table(self):
        """Create PDF documents table"""
        try:
            self.catalog.create_table(
                f"{self.namespace}.pdf_documents",
                schema=self.schemas.get_pdf_schema(),
                partition_spec=self.schemas.get_partition_spec()
            )
            logger.info("Created PDF documents table")
        except Exception as e:
            raise Exception(f"Failed to create PDF table: {e}")
    
    def _create_txt_table(self):
        """Create TXT documents table"""
        try:
            self.catalog.create_table(
                f"{self.namespace}.txt_documents",
                schema=self.schemas.get_txt_schema(),
                partition_spec=self.schemas.get_partition_spec()
            )
            logger.info("Created TXT documents table")
        except Exception as e:
            raise Exception(f"Failed to create TXT table: {e}")
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read data from specified table"""
        try:
            table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            return table.scan().to_pandas()
            
        except Exception as e:
            logger.error(f"Error reading {table_name}: {e}")
            traceback.print_exc()
            raise Exception(f"Failed to read table {table_name}: {e}")


# Singleton instance
table_manager = IcebergTableManager()