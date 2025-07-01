import os

class Settings():
    # Application
    app_name: str = os.getenv("APP_NAME", "ABC App")
    app_port: int = int(os.getenv("APP_PORT", 8000))
    debug_env = os.getenv("DEBUG", "false").lower()
    debug: bool = False if debug_env == "false" else True
    log_level: str = os.getenv("LOG_LEVEL")
    
    # MinIO Configuration
    minio_host: str = os.getenv("MINIO_HOST", "localhost")
    minio_port: int = int(os.getenv("MINIO_PORT", 9000))
    minio_endpoint: str = f"{minio_host}:{minio_port}"
    minio_endpoint_url: str = f"http://{minio_host}:{minio_port}"
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_secure_env = os.getenv("MINIO_SECURE", "false")
    minio_secure: bool = False if minio_secure_env == "false" else True
    
    # Iceberg Configuration
    iceberg_catalog_uri: str = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    iceberg_namespace: str = os.getenv("ICEBERG_NAMESPACE", "default")
    
    # Buckets
    staging_bucket: str = os.getenv("STAGING_BUCKET")
    silver_bucket: str = os.getenv("SILVER_BUCKET")
    lakehouse_bucket: str = os.getenv("LAKEHOUSE_BUCKET")
    crawled_bucket: str = os.getenv("CRAWLED_BUCKET", "crawled")
    
    # Processing Configuration
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    max_file_size_mb: int = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
    max_concurrent_processing: int = 5
    delete_after_processing: bool = False
    # Move to processed folder
    archive_after_processing: bool = True
    
    # Gemini Configuration
    gemini_api_key: str = os.getenv("GEMINI_API_KEY")
    
    # Vector Database Configuration
    qdrant_url: str = os.getenv("QDRANT_URL", "http://localhost:6333")
    qdrant_api_key: str = os.getenv("QDRANT_API_KEY")
    qdrant_collection_name = os.getenv("QDRANT_COLLECTION_NAME")


settings = Settings()