import sys
import os
import shutil

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from settings.config import settings
from api.endpoints import files, lakehouse, process, status
from logs import logger

# register startup and shutdown using lifespan Events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup event
    logger.info("Startup Event Triggered")
    logger.info(f"Starting {settings.app_name} Application")
    logger.info(f"Debug mode: {settings.debug}")
    logger.info(f"MinIO endpoint: {settings.minio_endpoint}")
    logger.info(f"Iceberg catalog URI: {settings.iceberg_catalog_uri}")
    
    # check temp folder
    if not os.path.exists("temp"):
        logger.info("Creating temp folder")
        os.makedirs("temp")
    else:
        logger.info("Temp folder already exists")
    
    yield

    # shutdown event
    logger.info("Shutdown Event Triggered")
    logger.info(f"Shutting down {settings.app_name} Application")

    # delete temp folder
    if os.path.exists("temp"):
        logger.info("Deleting temp folder")
        shutil.rmtree("temp")
    else:
        logger.info("Temp folder already deleted")

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description=f"{settings.app_name} API service",
    version="1.0.0",
    debug=settings.debug,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(files.router, prefix="/api")
app.include_router(lakehouse.router, prefix="/api")
app.include_router(process.router, prefix="/api")
app.include_router(status.router)
    

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": f"{settings.app_name} API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.app_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )