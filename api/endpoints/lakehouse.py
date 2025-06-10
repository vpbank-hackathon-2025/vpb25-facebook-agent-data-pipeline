from fastapi import APIRouter, HTTPException, status, Query
from typing import Optional, Dict, Any

from api.models.base import GenericResponseModel
from lakehouse.storage.tables import table_manager
from logs import logger

router = APIRouter(prefix="/lakehouse", tags=["lakehouse"])


@router.get("/tables", response_model=GenericResponseModel)
async def list_tables():
    """List all available tables in the lakehouse"""
    try:
        # Get available tables from catalog
        tables = table_manager.list_tables()
        
        return GenericResponseModel(
            message="Tables retrieved successfully",
            status_code=status.HTTP_200_OK,
            data={"tables": tables}
        )
        
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve tables list"
        )


@router.get("/tables/{table_name}", response_model=GenericResponseModel)
async def read_table(
    table_name: str,
    limit: Optional[int] = Query(None, description="Limit number of rows returned"),
    offset: Optional[int] = Query(0, description="Number of rows to skip")
):
    """Read a specific table from the lakehouse"""
    try:
        # Validate table name
        if not table_manager.table_exists(table_name):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table '{table_name}' not found"
            )
        
        # Read table data
        data_table_df = table_manager.read_table(table_name)
        
        # Apply pagination if specified
        if offset > 0:
            data_table_df = data_table_df.iloc[offset:]
        if limit is not None:
            data_table_df = data_table_df.head(limit)
        
        # Convert to dict and get metadata
        data_table_dict = data_table_df.to_dict(orient='records')
        total_rows = len(table_manager.read_table(table_name))  # Get total count
        
        return GenericResponseModel(
            message="Table read successfully",
            status_code=status.HTTP_200_OK,
            data={
                "table_name": table_name,
                "rows": data_table_dict,
                "total_rows": total_rows,
                "returned_rows": len(data_table_dict),
                "offset": offset,
                "limit": limit
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table '{table_name}'"
        )


@router.get("/tables/{table_name}/schema", response_model=GenericResponseModel)
async def get_table_schema(table_name: str):
    """Get schema information for a specific table"""
    try:
        # Validate table exists
        if not table_manager.table_exists(table_name):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table '{table_name}' not found"
            )
        
        # Get table schema
        schema_info = table_manager.get_table_schema(table_name)
        
        return GenericResponseModel(
            message="Table schema retrieved successfully",
            status_code=status.HTTP_200_OK,
            data={
                "table_name": table_name,
                "schema": schema_info
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting schema for table {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schema for table '{table_name}'"
        )


@router.get("/documents/{file_id}", response_model=GenericResponseModel)
async def get_document_by_id(
    file_id: str,
    table_name: Optional[str] = Query(None, description="Specific table to search in")
):
    """Get a specific document by its ID"""
    try:
        # Search in specific table or all document tables
        tables_to_search = []
        if table_name:
            if not table_manager.table_exists(table_name):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Table '{table_name}' not found"
                )
            tables_to_search = [table_name]
        else:
            # Search in both PDF and TXT tables
            tables_to_search = ["pdf_documents", "txt_documents"]
        
        document = None
        found_in_table = None
        
        for table in tables_to_search:
            if table_manager.table_exists(table):
                doc = table_manager.get_document_by_id(table, file_id)
                if doc is not None:
                    document = doc
                    found_in_table = table
                    break
        
        return GenericResponseModel(
            message="Document retrieved successfully",
            status_code=status.HTTP_200_OK,
            data={
                "file_id": file_id,
                "table": found_in_table,
                "document": document
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting document {file_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve document '{file_id}'"
        )


@router.get("/search", response_model=GenericResponseModel)
async def search_documents(
    query: str = Query(..., description="Search query"),
    table_name: Optional[str] = Query(None, description="Specific table to search in"),
    limit: Optional[int] = Query(10, description="Limit number of results")
):
    """Search documents by content"""
    try:
        # Determine tables to search
        tables_to_search = []
        if table_name:
            if not table_manager.table_exists(table_name):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Table '{table_name}' not found"
                )
            tables_to_search = [table_name]
        else:
            # Search in both PDF and TXT tables
            tables_to_search = ["pdf_documents", "txt_documents"]
        
        # Perform search
        results = table_manager.search_documents(
            query=query,
            tables=tables_to_search,
            limit=limit
        )
        
        return GenericResponseModel(
            message="Search completed successfully",
            status_code=status.HTTP_200_OK,
            data={
                "query": query,
                "total_results": len(results),
                "results": results
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search documents"
        )