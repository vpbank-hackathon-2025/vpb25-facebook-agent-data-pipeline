import traceback
from typing import List, Dict, Any, Optional
import pandas as pd

from lakehouse.connectors.iceberg import iceberg_connector
from lakehouse.storage.schemas import TableSchemas
from logs import logger


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
    
    def list_tables(self) -> List[str]:
        """List all tables in the namespace"""
        try:
            tables = self.catalog.list_tables(self.namespace)
            return tables
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error listing tables: {e}")
            raise Exception(f"Failed to list tables: {e}")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        return iceberg_connector.table_exists(table_name)
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        try:
            table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            schema = table.schema()
            
            return {
                "fields": [
                    {
                        "id": field.field_id,
                        "name": field.name,
                        "type": str(field.field_type),
                        "required": field.required
                    }
                    for field in schema.fields
                ]
            }
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            raise Exception(f"Failed to get schema for {table_name}: {e}")
    
    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Get table statistics"""
        try:
            df = self.read_table(table_name)
            
            return {
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "memory_usage": df.memory_usage(deep=True).sum(),
                "null_counts": df.isnull().sum().to_dict()
            }
        except Exception as e:
            logger.error(f"Error getting statistics for {table_name}: {e}")
            raise Exception(f"Failed to get statistics for {table_name}: {e}")
    
    def get_document_by_id(self, table_name: str, file_id: str) -> Optional[Dict[str, Any]]:
        """Get document by file_id"""
        try:
            df = self.read_table(table_name)
            result = df[df['file_id'] == file_id]
            
            if len(result) == 0:
                return None
            
            return result.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"Error getting document {file_id} from {table_name}: {e}")
            raise Exception(f"Failed to get document {file_id}: {e}")
    
    def search_documents(self, query: str, tables: List[str], limit: int = 10) -> List[Dict[str, Any]]:
        """Search documents by content"""
        try:
            all_results = []
            
            for table_name in tables:
                if not self.table_exists(table_name):
                    continue
                    
                df = self.read_table(table_name)
                
                # Simple text search in content column
                if 'content' in df.columns:
                    mask = df['content'].str.contains(query, case=False, na=False)
                    results = df[mask]
                    
                    # Add table source info
                    results_dict = results.to_dict(orient='records')
                    for result in results_dict:
                        result['source_table'] = table_name
                    
                    all_results.extend(results_dict)
            
            # Sort by relevance (you can implement more sophisticated scoring)
            # For now, just return first `limit` results
            return all_results[:limit]
            
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            raise Exception(f"Failed to search documents: {e}")
    
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