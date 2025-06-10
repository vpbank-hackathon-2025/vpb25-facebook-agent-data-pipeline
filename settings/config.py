import os

class Settings():
    # Application
    app_name: str = os.getenv("APP_NAME", "ABC App")
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
    
    # Processing Configuration
    batch_size: int = int(os.getenv("BATCH_SIZE"))
    max_file_size_mb: int = int(os.getenv("MAX_FILE_SIZE_MB"))
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()