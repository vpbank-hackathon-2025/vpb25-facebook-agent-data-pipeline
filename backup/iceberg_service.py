import os
import traceback

from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, TimestampType, LongType, IntegerType, 
    ListType, NestedField
)

from core.config import settings
from models.schemas import PDFDocument, TXTDocument
from logs import logger


class IcebergService:
    def __init__(self):
        self.catalog = None
        self.namespace = settings.iceberg_namespace
        self._initialize_catalog()
    
    def _initialize_catalog(self):
        """Initialize Iceberg catalog"""
        try:
            # Configure catalog - Updated configuration for localhost connection
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
            self._ensure_tables()
            logger.info("Iceberg catalog initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {e}")
            traceback.print_exc()
            raise
    
    def _ensure_tables(self):
        """Ensure PDF and TXT tables exist"""
        try:
            # Create PDF documents table if not exists
            if not self._table_exists("pdf_documents"):
                self._create_pdf_table()
            
            # Create TXT documents table if not exists
            if not self._table_exists("txt_documents"):
                self._create_txt_table()
                
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {e}")
            traceback.print_exc()
            raise
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in catalog"""
        try:
            self.catalog.load_table(f"{self.namespace}.{table_name}")
            return True
        except:
            return False
    
    def _create_pdf_table(self):
        """Create PDF documents table"""
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import MonthTransform

        # Define schema
        schema = Schema(
            NestedField(1, "file_id", StringType(), required=True),
            NestedField(2, "datetime", TimestampType(), required=True),
            NestedField(3, "upload_datetime", TimestampType(), required=True),
            NestedField(4, "title", StringType(), required=True),
            NestedField(5, "content", StringType(), required=True),
            NestedField(6, "source_file", StringType(), required=True),
            NestedField(7, "file_size", LongType(), required=True),
            NestedField(8, "content_hash", StringType(), required=True),
            NestedField(9, "page_count", IntegerType(), required=False),
            NestedField(10, "version", IntegerType(), required=True)
        )
        
        # Create table with partitioning
        partition_spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1001, transform=MonthTransform(), name="month")
        )
        
        self.catalog.create_table(
            f"{self.namespace}.pdf_documents",
            schema=schema,
            partition_spec=partition_spec
        )
        logger.info("Created PDF documents table")
    
    def _create_txt_table(self):
        """Create TXT documents table"""
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import MonthTransform
        
        schema = Schema(
            NestedField(1, "file_id", StringType(), required=True),
            NestedField(2, "datetime", TimestampType(), required=True),
            NestedField(3, "upload_datetime", TimestampType(), required=True),
            NestedField(4, "title", StringType(), required=True),
            NestedField(5, "content", StringType(), required=True),
            NestedField(6, "source_file", StringType(), required=True),
            NestedField(7, "source_link", StringType(), required=False),
            NestedField(8, "file_size", LongType(), required=True),
            NestedField(9, "content_hash", StringType(), required=True),
            NestedField(10, "version", IntegerType(), required=True)
        )
        
        # Create table with partitioning
        partition_spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1001, transform=MonthTransform(), name="month")
        )
        
        self.catalog.create_table(
            f"{self.namespace}.txt_documents",
            schema=schema,
            partition_spec=partition_spec
        )
        logger.info("Created TXT documents table")
    
    def insert_pdf_documents(self, documents: List[PDFDocument]) -> bool:
        """Insert PDF documents into Iceberg table"""
        try:
            if not documents:
                return True
            table = self.catalog.load_table(f"{self.namespace}.pdf_documents")
            
            # Prepare data dictionary
            data_dict = {
                'file_id': [],
                'datetime': [],
                'upload_datetime': [],
                'title': [],
                'content': [],
                'source_file': [],
                'file_size': [],
                'content_hash': [],
                'page_count': [],
                'tags': [],
                'version': []
            }
            
            for doc in documents:
                data_dict['file_id'].append(doc.file_id)
                data_dict['datetime'].append(doc.datetime)
                data_dict['upload_datetime'].append(doc.upload_datetime)
                data_dict['title'].append(doc.title)
                data_dict['content'].append(doc.content)
                data_dict['source_file'].append(doc.source_file)
                data_dict['file_size'].append(doc.file_size)
                data_dict['content_hash'].append(doc.content_hash)
                data_dict['page_count'].append(doc.page_count)
                data_dict['version'].append(doc.version)
            
            # Define schema
            schema = pa.schema([
                pa.field('file_id', pa.string(), nullable=False),
                pa.field('datetime', pa.timestamp('us'), nullable=False),
                pa.field('upload_datetime', pa.timestamp('us'), nullable=False),
                pa.field('title', pa.string(), nullable=False),
                pa.field('content', pa.string(), nullable=False),
                pa.field('source_file', pa.string(), nullable=False),
                pa.field('file_size', pa.int64(), nullable=False),
                pa.field('content_hash', pa.string(), nullable=False),
                pa.field('page_count', pa.int32(), nullable=True),
                pa.field('version', pa.int32(), nullable=False)
            ])
            
            # Create PyArrow table from dictionary
            pyarrow_table = pa.table(data_dict, schema=schema)
            table.append(pyarrow_table)
            
            logger.info(f"Inserted {len(documents)} PDF documents")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting PDF documents: {e}")
            traceback.print_exc()
            return False

    def insert_txt_documents(self, documents: List[TXTDocument]) -> bool:
        """Insert TXT documents into Iceberg table"""
        try:
            if not documents:
                return True
                
            table = self.catalog.load_table(f"{self.namespace}.txt_documents")
            
            # Prepare data dictionary
            data_dict = {
                'file_id': [],
                'datetime': [],
                'upload_datetime': [],
                'title': [],
                'content': [],
                'source_file': [],
                'source_link': [],
                'file_size': [],
                'content_hash': [],
                'version': []
            }
            
            for doc in documents:
                data_dict['file_id'].append(doc.file_id)
                data_dict['datetime'].append(doc.datetime)
                data_dict['upload_datetime'].append(doc.upload_datetime)
                data_dict['title'].append(doc.title)
                data_dict['content'].append(doc.content)
                data_dict['source_file'].append(doc.source_file)
                data_dict['source_link'].append(doc.source_link)
                data_dict['file_size'].append(doc.file_size)
                data_dict['content_hash'].append(doc.content_hash)
                data_dict['version'].append(doc.version)
            
            # Define schema
            schema = pa.schema([
                pa.field('file_id', pa.string(), nullable=False),
                pa.field('datetime', pa.timestamp('us'), nullable=False),
                pa.field('upload_datetime', pa.timestamp('us'), nullable=False),
                pa.field('title', pa.string(), nullable=False),
                pa.field('content', pa.string(), nullable=False),
                pa.field('source_file', pa.string(), nullable=False),
                pa.field('source_link', pa.string(), nullable=True),
                pa.field('file_size', pa.int64(), nullable=False),
                pa.field('content_hash', pa.string(), nullable=False),
                pa.field('version', pa.int32(), nullable=False)
            ])
            
            # Create PyArrow table from dictionary
            pyarrow_table = pa.table(data_dict, schema=schema)
            table.append(pyarrow_table)
            
            logger.info(f"Inserted {len(documents)} TXT documents")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting TXT documents: {e}")
            traceback.print_exc()
            return False

    def read_table(self, table_name) -> pd.DataFrame:
        """Get data in table_name"""
        try:
            data_table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            data_table_df = data_table.scan().to_pandas()
                
            return data_table_df
            
        except Exception as e:
            logger.error(f"Error reading {table_name}: {e}")
            traceback.print_exc()
            return {'error': str(e)}


# Singleton instance
iceberg_service = IcebergService()