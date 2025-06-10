from lakehouse.processors.batch import BatchProcessor, create_batch_processor


def get_batch_processor() -> BatchProcessor:
    """Dependency to get BatchProcessor instance"""
    return create_batch_processor()