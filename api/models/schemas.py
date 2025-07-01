from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

class DocumentBase(BaseModel):
    title: str
    content: str
    source_file: str


class PDFDocument(DocumentBase):
    file_id: str
    upload_datetime: datetime
    title: str
    summarize_details: Optional[str] = None
    content: str
    source_file: str
    file_size: int
    page_count: Optional[int] = None
    version: int = 1


class TXTDocument(DocumentBase):
    file_id: str
    upload_datetime: datetime
    file_size: int
    source_link: Optional[str] = None
    version: int = 1


class ProcessingStatus(BaseModel):
    file_id: str
    filename: str
    status: str  # "pending", "processing", "completed", "failed"
    message: Optional[str] = None
    processed_at: Optional[datetime] = None

