import pytz
import http
import uuid

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from datetime import datetime

from models.base import GenericResponseModel
from logs import logger

def get_vietnam_timezone():
    """
    Get the timezone object for Vietnam.
    
    Returns:
        pytz.timezone: Timezone object for Vietnam.
    """
    return pytz.timezone("Asia/Ho_Chi_Minh")

def get_current_vietnam_timestamp():
    """
    Get the current timestamp in Vietnam timezone.
    
    Returns:
        timestamp: Current timestamp in Vietnam timezone.
    """
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    # YYYYMMDD_HHMMSS format
    vietnam_timestamp = datetime.now(vietnam_tz)
    return vietnam_timestamp.timestamp()

def get_current_vietnam_datetime():
    """
    Get the current datetime in Vietnam timezone.
    
    Returns:
        datetime: Current datetime in Vietnam timezone.
    """
    vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    vietnam_datetime = datetime.now(vietnam_tz)
    return vietnam_datetime

def build_api_response(generic_response: GenericResponseModel) -> JSONResponse:
    try:
        if not generic_response.api_id:
            generic_response.api_id = str(uuid.uuid4().hex)
        if not generic_response.status_code:
            generic_response.status_code = http.HTTPStatus.OK if not generic_response.error \
                else http.HTTPStatus.UNPROCESSABLE_ENTITY
        response_json = jsonable_encoder(generic_response)
        res = JSONResponse(status_code=generic_response.status_code, content=response_json)
        logger.info(msg=f"build_api_response: Generated Response with status_code:{generic_response.status_code}")
        return res
    except Exception as e:
        logger.error(msg=f"exception in build_api_response error : {e}")
        return JSONResponse(status_code=generic_response.status_code, content=generic_response.error)