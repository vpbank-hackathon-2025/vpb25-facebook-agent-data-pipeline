import http

from fastapi import APIRouter, File, UploadFile, HTTPException, status

from core.config import settings
from models.base import GenericResponseModel
from utils.helper import build_api_response
from services.minio_service import minio_service
from services.pdf_service import pdf_service
from services.txt_service import txt_service
from logs import logger

router = APIRouter(prefix="/files", tags=["files"])

@router.get("/pdf", response_model=GenericResponseModel)
async def get_all_pdf_files():
    """Get all PDF files from staging bucket"""
    try:
        files = minio_service.list_files(settings.staging_bucket, "pdfs")
        res =  GenericResponseModel(
            message="PDF files retrieved successfully",
            status_code=http.HTTPStatus.OK,
            data = {
                "files": files,
                "total_count": len(files)
            }
        )
        return build_api_response(res)
        
    except Exception as e:
        logger.error(f"Error listing PDF files: {e}")
        res =  GenericResponseModel(
            message="Failed to retrieve PDF files",
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR
        )
        return build_api_response(res)


@router.get("/txt", response_model=GenericResponseModel)
async def get_all_txt_files():
    """Get all TXT files from staging bucket"""
    try:
        files = minio_service.list_files(settings.staging_bucket, "txts")
        res =  GenericResponseModel(
            message="TXT files retrieved successfully",
            status_code=http.HTTPStatus.OK,
            data = {
                "files": files,
                "total_count": len(files)
            }
        )
        return build_api_response(res)
    except Exception as e:
        logger.error(f"Error listing TXT files: {e}")
        res =  GenericResponseModel(
            message="Failed to retrieve TXT files",
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR
        )
        return build_api_response(res)


@router.post("/upload/pdf", response_model=GenericResponseModel)
async def upload_pdf(file: UploadFile = File(...)):
    """Upload PDF file to staging bucket"""
    try:
        # Validate file
        if not file.filename.lower().endswith('.pdf'):
            res =  GenericResponseModel(
                message="Only PDF files are allowed",
                status_code=http.HTTPStatus.BAD_REQUEST
            )
            return build_api_response(res)
        
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        max_size_bytes = settings.max_file_size_mb * 1024 * 1024
        if len(file_content) > max_size_bytes:
            res =  GenericResponseModel(
                message=f"File size exceeds {settings.max_file_size_mb}MB limit",
                status_code=http.HTTPStatus.BAD_REQUEST
            )
            return build_api_response(res)
        
        # Validate PDF content
        is_valid, error_msg = pdf_service.validate_pdf(file_content)
        if not is_valid:
            res =  GenericResponseModel(
                message=error_msg,
                status_code=http.HTTPStatus.BAD_REQUEST
            )
            return build_api_response(res)
        
        # Upload to MinIO
        object_name = f"pdfs/{file.filename}"
        success = minio_service.upload_file(
            settings.staging_bucket,
            object_name,
            file_content,
            content_type="application/pdf"
        )
        
        if not success:
            res =  GenericResponseModel(
                message="Failed to upload file",
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR
            )
            return build_api_response(res)

        res =  GenericResponseModel(
            message="PDF file uploaded successfully",
            status_code=http.HTTPStatus.OK,
            data = {
                "filename": file.filename,
                "file_size": len(file_content),
                "upload_path": object_name
            }
        )
        return build_api_response(res)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading PDF: {e}")
        res =  GenericResponseModel(
            message="Internal server error during file upload",
            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR
        )
        return build_api_response(res)


@router.post("/upload/txt", response_model=GenericResponseModel)
async def upload_txt(file: UploadFile = File(...)):
    """Upload TXT file to staging bucket"""
    try:
        # Validate file
        if not file.filename.lower().endswith('.txt'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only TXT files are allowed"
            )
        
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        max_size_bytes = settings.max_file_size_mb * 1024 * 1024
        if len(file_content) > max_size_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds {settings.max_file_size_mb}MB limit"
            )
        
        # Validate TXT content
        is_valid, error_msg = txt_service.validate_txt(file_content)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )
        
        # Upload to MinIO
        object_name = f"txts/{file.filename}"
        success = minio_service.upload_file(
            settings.staging_bucket,
            object_name,
            file_content,
            content_type="text/plain"
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload file"
            )
        
        res =  GenericResponseModel(
            message="TXT file uploaded successfully",
            status_code=http.HTTPStatus.OK,
            data = {
                "filename": file.filename,
                "file_size": len(file_content),
                "upload_path": object_name
            }
        )

        return build_api_response(res)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading TXT: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during file upload"
        )
