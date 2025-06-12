from fastapi import APIRouter, File, UploadFile, HTTPException, status, Query

from api.models.base import GenericResponseModel
from lakehouse.storage.file_manager import file_manager
from logs import logger
from settings.config import settings
from api.dependencies import get_file_validator
from utils.helper import build_api_response

router = APIRouter(prefix="/files", tags=["files"])


@router.get("/pdf", response_model=GenericResponseModel)
async def get_all_pdf_files():
    """Get all PDF files from staging bucket"""
    try:
        files = file_manager.list_files(
            settings.staging_bucket, 
            "pdfs/", 
            include_metadata=True
        )
        
        res = GenericResponseModel(
            message="PDF files retrieved successfully",
            status_code=status.HTTP_200_OK,
            data={
                "files": files,
                "total_count": len(files)
            }
        )
        return build_api_response(res)
    except Exception as e:
        logger.error(f"Error listing PDF files: {e}")
        return build_api_response(
            GenericResponseModel(
                message="Failed to retrieve PDF files",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=True
            )
        )


@router.get("/txt", response_model=GenericResponseModel)
async def get_all_txt_files():
    """Get all TXT files from staging bucket"""
    try:
        files = file_manager.list_files(
            settings.staging_bucket, 
            "txts/", 
            include_metadata=True
        )
        
        return GenericResponseModel(
            message="TXT files retrieved successfully",
            status_code=status.HTTP_200_OK,
            data={
                "files": files,
                "total_count": len(files)
            }
        )
        
    except Exception as e:
        logger.error(f"Error listing TXT files: {e}")
        return build_api_response(
            GenericResponseModel(
                message="Failed to retrieve TXT files",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=True
            )
        )


@router.get("/all", response_model=GenericResponseModel)
async def get_all_files(
    bucket_name: str,
    file_type: str = Query(None, description="Filter by file type (pdf, txt, ...)")
):
    """Get all files from staging bucket with optional filtering"""
    try:
        all_files = []
        
        if file_type is None or file_type.lower() == "pdf":
            pdf_files = file_manager.list_files(
                bucket_name, 
                "pdfs/", 
                include_metadata=True
            )
            for file in pdf_files:
                file["type"] = "pdf"
            all_files.extend(pdf_files)
        
        if file_type is None or file_type.lower() == "txt":
            txt_files = file_manager.list_files(
                bucket_name, 
                "txts/", 
                include_metadata=True
            )
            for file in txt_files:
                file["type"] = "txt"
            all_files.extend(txt_files)
        
        return GenericResponseModel(
            message="Files retrieved successfully",
            status_code=status.HTTP_200_OK,
            data={
                "files": all_files,
                "total_count": len(all_files),
                "filter": file_type
            }
        )
        
    except Exception as e:
        logger.error(f"Error listing all files: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve files: {e}"
        )


@router.post("/upload/pdf", response_model=GenericResponseModel)
async def upload_pdf(file: UploadFile = File(...)):
    """Upload PDF file to staging bucket"""
    try:
        validator = get_file_validator()
        
        # Validate file extension
        if not file.filename.lower().endswith('.pdf'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only PDF files are allowed"
            )
        
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        validator.validate_file_size(file_content, file.filename)
        
        # Validate PDF content
        validator.validate_pdf_content(file_content)
        
        # Check if file already exists
        object_name = f"pdfs/{file.filename}"
        if file_manager.file_exists(settings.staging_bucket, object_name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"File '{file.filename}' already exists"
            )
        
        # Upload to MinIO
        success = file_manager.upload_file(
            bucket=settings.staging_bucket,
            object_name=object_name,
            file_data=file_content,
            content_type="application/pdf",
            metadata={
                "original_filename": file.filename,
                "upload_source": "api"
            }
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload file"
            )

        return GenericResponseModel(
            message="PDF file uploaded successfully",
            status_code=status.HTTP_201_CREATED,
            data={
                "filename": file.filename,
                "file_size": len(file_content),
                "upload_path": object_name,
                "content_type": "application/pdf"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading PDF: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during file upload"
        )


@router.post("/upload/txt", response_model=GenericResponseModel)
async def upload_txt(file: UploadFile = File(...)):
    """Upload TXT file to staging bucket"""
    try:
        validator = get_file_validator()
        
        # Validate file extension
        if not file.filename.lower().endswith('.txt'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only TXT files are allowed"
            )
        
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        validator.validate_file_size(file_content, file.filename)
        
        # Validate TXT content
        validator.validate_txt_content(file_content)
        
        # Check if file already exists
        object_name = f"txts/{file.filename}"
        if file_manager.file_exists(settings.staging_bucket, object_name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"File '{file.filename}' already exists"
            )
        
        # Upload to MinIO
        success = file_manager.upload_file(
            bucket=settings.staging_bucket,
            object_name=object_name,
            file_data=file_content,
            content_type="text/plain",
            metadata={
                "original_filename": file.filename,
                "upload_source": "api"
            }
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload file"
            )
        
        return GenericResponseModel(
            message="TXT file uploaded successfully",
            status_code=status.HTTP_201_CREATED,
            data={
                "filename": file.filename,
                "file_size": len(file_content),
                "upload_path": object_name,
                "content_type": "text/plain"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading TXT: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during file upload"
        )


@router.delete("/delete/{file_type}/{filename}", response_model=GenericResponseModel)
async def delete_file(file_path: str):
    """Delete a file from staging bucket"""
    try:
        # Construct object path
        object_name = file_path
        file_name = file_path.split("/")[-1]
        
        # Check if file exists
        if not file_manager.file_exists(settings.staging_bucket, object_name):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File '{file_path}' not found"
            )
        
        # Delete file
        success = file_manager.delete_file(settings.staging_bucket, object_name)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete file"
            )
        
        return GenericResponseModel(
            message="File deleted successfully",
            status_code=status.HTTP_200_OK,
            data={
                "filename": file_name,
                "deleted_path": object_name
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting file {file_path}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during file deletion"
        )


@router.get("/info/{file_type}/{filename}", response_model=GenericResponseModel)
async def get_file_info(file_type: str, filename: str):
    """Get information about a specific file"""
    try:
        # Validate file type
        if file_type.lower() not in ["pdf", "txt"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File type must be 'pdf' or 'txt'"
            )
        
        # Construct object path
        object_name = f"{file_type.lower()}s/{filename}"
        
        # Check if file exists
        if not file_manager.file_exists(settings.staging_bucket, object_name):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File '{filename}' not found"
            )
        
        # Get file info
        file_info = file_manager.get_file_info(settings.staging_bucket, object_name)
        
        return GenericResponseModel(
            message="File information retrieved successfully",
            status_code=status.HTTP_200_OK,
            data=file_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting file info {filename}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error while retrieving file information"
        )