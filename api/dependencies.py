from lakehouse.processors.batch import BatchProcessor, create_batch_processor
from lakehouse.processors.validators import FileValidator, create_file_validator

def get_batch_processor() -> BatchProcessor:
    """Dependency to get BatchProcessor instance"""
    return create_batch_processor()

def get_file_validator() -> FileValidator:
    """Dependency to get FileValidator instance"""
    return create_file_validator()