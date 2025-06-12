import uuid

from http import HTTPStatus
from typing import Annotated, Any, Optional
from pydantic import BaseModel

class GenericResponseModel(BaseModel):
    """Generic response model for all responses"""
    api_id: Annotated[Optional[str], "API Id"] = str(uuid.uuid4().hex)
    error: Annotated[bool, "Error"] = False
    message: Annotated[Optional[str], "Message"] = None
    data: Any = None
    status_code: Annotated[Optional[int], "Status Code"] = HTTPStatus.OK
