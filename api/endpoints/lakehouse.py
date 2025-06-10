from fastapi import APIRouter, HTTPException, status

from services.iceberg_service import iceberg_service
from logs import logger
from models.base import GenericResponseModel
from utils.helper import build_api_response

router = APIRouter(prefix="/lakehouse", tags=["lakehouse"])

@router.get("/read_table", response_model=GenericResponseModel)
async def read_table(table_name: str):
    """Read a specific table from the lakehouse"""
    try:
        data_table_df = iceberg_service.read_table(table_name)
        data_table_dict = data_table_df.to_dict(orient='records')
        res = GenericResponseModel(
            message="Table read successfully",
            status_code=status.HTTP_200_OK,
            data=data_table_dict
        )
        return build_api_response(res)
        
    except Exception as e:
        logger.error(f"Error getting document stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve document statistics"
        )

@router.get("/{file_id}")
async def get_document_by_id(table_name: str, file_id: str):
    """Get a specific document by its ID"""
    pass
    # try:
        
    #     results = iceberg_service.search_documents(search_request)
        
    #     if not results['documents']:
    #         raise HTTPException(
    #             status_code=status.HTTP_404_NOT_FOUND,
    #             detail=f"Document with ID {file_id} not found"
    #         )
        
    #     # Find exact match by file_id
    #     for doc in results['documents']:
    #         if doc.get('file_id') == file_id:
    #             return doc
        
    #     raise HTTPException(
    #         status_code=status.HTTP_404_NOT_FOUND,
    #         detail=f"Document with ID {file_id} not found"
    #     )
        
    # except HTTPException:
    #     raise
    # except Exception as e:
    #     logger.error(f"Error getting document {file_id}: {e}")
    #     raise HTTPException(
    #         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    #         detail="Failed to retrieve document"
    #     )
