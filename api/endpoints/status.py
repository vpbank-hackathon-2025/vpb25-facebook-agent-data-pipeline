import http

from fastapi import APIRouter

from logs import logger
from models.base import GenericResponseModel
from utils.helper import build_api_response, get_current_vietnam_datetime

router = APIRouter(tags=["status"])

@router.get('/health', tags=['status'], response_model=GenericResponseModel)
async def health_check():
    """Health check endpoint"""
    try:
        vietnam_timestamp = get_current_vietnam_datetime()
        # convert to YYYY-MM-DD HH:MM:SS
        vietnam_timestamp = vietnam_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        res = GenericResponseModel(
            message="Service is up and running",
            data={
                "status": "healthy",
                "timestamp": vietnam_timestamp
                },
            status_code=http.HTTPStatus.OK
        )
        return build_api_response(res)
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return build_api_response(GenericResponseModel(
            message="Service is down",
            data={
                "status": "unhealthy",
                "timestamp": get_current_vietnam_datetime().strftime("%Y-%m-%d %H:%M:%S")
            },
            status_code=http.HTTPStatus.SERVICE_UNAVAILABLE
        ))