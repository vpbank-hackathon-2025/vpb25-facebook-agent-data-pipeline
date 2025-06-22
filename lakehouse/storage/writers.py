import traceback
from typing import List
import pyarrow as pa

from lakehouse.connectors.iceberg import iceberg_connector
from logs import logger
from api.models.schemas import PDFDocument, TXTDocument


class IcebergWriter:
    """Handles writing data to Iceberg tables"""
    
    def __init__(self):
        self.catalog = iceberg_connector.get_catalog()
        self.namespace = iceberg_connector.namespace
    
    def insert_pdf_documents(self, documents: List[PDFDocument]) -> bool:
        """Insert PDF documents into Iceberg table"""
        try:
            if not documents:
                return True
                
            table = self.catalog.load_table(f"{self.namespace}.pdf_documents")
            
            # Prepare data dictionary
            data_dict = self._prepare_pdf_data(documents)
            
            # Create PyArrow table
            schema = self._get_pdf_arrow_schema()
            pyarrow_table = pa.table(data_dict, schema=schema)
            
            # Insert data
            table.append(pyarrow_table)
            
            logger.info(f"Inserted {len(documents)} PDF documents")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting PDF documents: {e}")
            traceback.print_exc()
            raise Exception(f"Failed to insert PDF documents: {e}")
    
    def insert_txt_documents(self, documents: List[TXTDocument]) -> bool:
        """Insert TXT documents into Iceberg table"""
        try:
            if not documents:
                return True
                
            table = self.catalog.load_table(f"{self.namespace}.txt_documents")
            
            # Prepare data dictionary
            data_dict = self._prepare_txt_data(documents)
            
            # Create PyArrow table
            schema = self._get_txt_arrow_schema()
            pyarrow_table = pa.table(data_dict, schema=schema)
            
            # Insert data
            table.append(pyarrow_table)
            
            logger.info(f"Inserted {len(documents)} TXT documents")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting TXT documents: {e}")
            traceback.print_exc()
            raise Exception(f"Failed to insert TXT documents: {e}")
    
    def _prepare_pdf_data(self, documents: List[PDFDocument]) -> dict:
        """Prepare PDF documents data for insertion"""
        data_dict = {
            'file_id': [], 'upload_datetime': [],
            'title': [], 'content': [], 'source_file': [],
            'file_size': [], 'page_count': [],
            'version': []
        }
        
        for doc in documents:
            data_dict['file_id'].append(doc.file_id)
            data_dict['upload_datetime'].append(doc.upload_datetime)
            data_dict['title'].append(doc.title)
            data_dict['content'].append(doc.content)
            data_dict['source_file'].append(doc.source_file)
            data_dict['file_size'].append(doc.file_size)
            data_dict['page_count'].append(doc.page_count)
            data_dict['version'].append(doc.version)
        
        return data_dict
    
    def _prepare_txt_data(self, documents: List[TXTDocument]) -> dict:
        """Prepare TXT documents data for insertion"""
        data_dict = {
            'file_id': [], 'upload_datetime': [],
            'title': [], 'content': [], 'source_file': [],
            'source_link': [], 'file_size': [],
            'version': []
        }
        
        for doc in documents:
            data_dict['file_id'].append(doc.file_id)
            data_dict['upload_datetime'].append(doc.upload_datetime)
            data_dict['title'].append(doc.title)
            data_dict['content'].append(doc.content)
            data_dict['source_file'].append(doc.source_file)
            data_dict['source_link'].append(doc.source_link)
            data_dict['file_size'].append(doc.file_size)
            data_dict['content_hash'].append(doc.content_hash)
            data_dict['version'].append(doc.version)
        
        return data_dict
    
    def _get_pdf_arrow_schema(self) -> pa.Schema:
        """Get PyArrow schema for PDF documents"""
        return pa.schema([
            pa.field('file_id', pa.string(), nullable=False),
            pa.field('upload_datetime', pa.timestamp('us'), nullable=False),
            pa.field('title', pa.string(), nullable=False),
            pa.field('content', pa.string(), nullable=False),
            pa.field('source_file', pa.string(), nullable=False),
            pa.field('file_size', pa.int64(), nullable=False),
            pa.field('page_count', pa.int32(), nullable=True),
            pa.field('version', pa.int32(), nullable=False)
        ])
    
    def _get_txt_arrow_schema(self) -> pa.Schema:
        """Get PyArrow schema for TXT documents"""
        return pa.schema([
            pa.field('file_id', pa.string(), nullable=False),
            pa.field('upload_datetime', pa.timestamp('us'), nullable=False),
            pa.field('title', pa.string(), nullable=False),
            pa.field('content', pa.string(), nullable=False),
            pa.field('source_file', pa.string(), nullable=False),
            pa.field('source_link', pa.string(), nullable=True),
            pa.field('file_size', pa.int64(), nullable=False),
            pa.field('version', pa.int32(), nullable=False)
        ])


# Singleton instance
iceberg_writer = IcebergWriter()