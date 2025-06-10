"""Processing endpoints for batch operations"""
from fastapi import APIRouter, HTTPException, status

from api.models.base import GenericResponseModel
from api.models.schemas import ProcessingStatus
from lakehouse.processors.batch import BatchProcessor
from logs import logger
from api.dependencies import get_batch_processor

router = APIRouter(prefix="/process", tags=["process"])


@router.post("/batch", response_model=GenericResponseModel)
async def process_batch():
    """Process all files in staging bucket and move to lakehouse"""
    try:
        batch_processor = get_batch_processor()
        result = await batch_processor.process_all_files()
        
        return GenericResponseModel(
            message="Batch processed successfully",
            status_code=status.HTTP_200_OK,
            data=result
        )
        
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process batch files"
        )


@router.post("/batch/pdf", response_model=GenericResponseModel)
async def process_pdf_batch():
    """Process only PDF files in staging bucket"""
    try:
        batch_processor = get_batch_processor()
        result = await batch_processor.process_pdf_files()
        
        return GenericResponseModel(
            message="PDF batch processed successfully",
            status_code=status.HTTP_200_OK,
            data=result
        )
        
    except Exception as e:
        logger.error(f"Error processing PDF batch: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process PDF files"
        )


@router.post("/batch/txt", response_model=GenericResponseModel)
async def process_txt_batch():
    """Process only TXT files in staging bucket"""
    try:
        batch_processor = get_batch_processor()
        result = await batch_processor.process_txt_files()
        
        return GenericResponseModel(
            message="TXT batch processed successfully", 
            status_code=status.HTTP_200_OK,
            data=result
        )
        
    except Exception as e:
        logger.error(f"Error processing TXT batch: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process TXT files"
        )