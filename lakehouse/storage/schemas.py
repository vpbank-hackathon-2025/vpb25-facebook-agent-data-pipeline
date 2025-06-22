from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, TimestampType, LongType, IntegerType, 
    NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform


class TableSchemas:
    """Centralized table schema definitions"""
    
    @staticmethod
    def get_pdf_schema() -> Schema:
        """Get PDF documents table schema"""
        return Schema(
            NestedField(1, "file_id", StringType(), required=True),
            NestedField(3, "upload_datetime", TimestampType(), required=True),
            NestedField(4, "title", StringType(), required=True),
            NestedField(5, "content", StringType(), required=True),
            NestedField(6, "source_file", StringType(), required=True),
            NestedField(7, "file_size", LongType(), required=True),
            NestedField(9, "page_count", IntegerType(), required=False),
            NestedField(10, "version", IntegerType(), required=True)
        )
    
    @staticmethod
    def get_txt_schema() -> Schema:
        """Get TXT documents table schema"""
        return Schema(
            NestedField(1, "file_id", StringType(), required=True),
            NestedField(3, "upload_datetime", TimestampType(), required=True),
            NestedField(4, "title", StringType(), required=True),
            NestedField(5, "content", StringType(), required=True),
            NestedField(6, "source_file", StringType(), required=True),
            NestedField(7, "source_link", StringType(), required=False),
            NestedField(8, "file_size", LongType(), required=True),
            NestedField(10, "version", IntegerType(), required=True)
        )
    
    @staticmethod
    def get_partition_spec() -> PartitionSpec:
        """Get standard partitioning specification"""
        return PartitionSpec(
            PartitionField(
                source_id=3, 
                field_id=1001, 
                transform=MonthTransform(), 
                name="month"
            )
        )