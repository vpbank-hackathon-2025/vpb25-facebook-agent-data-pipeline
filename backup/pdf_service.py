import io
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any
import PyPDF2
import pdfplumber

from models.schemas import PDFDocument
from utils.text_utils import clean_text, extract_title_from_content
from logs import logger
from utils.helper import get_current_vietnam_timestamp


class PDFService:
    def __init__(self):
        pass
    
    def extract_text_content(self, pdf_data: bytes) -> Dict[str, Any]:
        """Extract text content from PDF using multiple methods"""
        try:
            # Try with pdfplumber first (better for complex layouts)
            content = self._extract_with_pdfplumber(pdf_data)
            page_count = self._get_page_count_pdfplumber(pdf_data)
            
            if not content.strip():
                # Fallback to PyPDF2
                content = self._extract_with_pypdf2(pdf_data)
                page_count = self._get_page_count_pypdf2(pdf_data)
            
            if not content.strip():
                logger.warning("No text content extracted from PDF")
                content = "[No text content found]"
            
            return {
                "content": clean_text(content),
                "page_count": page_count
            }
        
        except Exception as e:
            logger.error(f"Error extracting PDF content: {e}")
            return {
                "content": "[Error extracting content]",
                "page_count": 0
            }
    
    def _extract_with_pdfplumber(self, pdf_data: bytes) -> str:
        """Extract text using pdfplumber"""
        content_parts = []
        
        with io.BytesIO(pdf_data) as pdf_stream:
            with pdfplumber.open(pdf_stream) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        content_parts.append(text)
        
        return "\n\n".join(content_parts)
    
    def _extract_with_pypdf2(self, pdf_data: bytes) -> str:
        """Extract text using PyPDF2"""
        content_parts = []
        
        with io.BytesIO(pdf_data) as pdf_stream:
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            
            for page in pdf_reader.pages:
                text = page.extract_text()
                if text:
                    content_parts.append(text)
        
        return "\n\n".join(content_parts)
    
    def _get_page_count_pdfplumber(self, pdf_data: bytes) -> int:
        """Get page count using pdfplumber"""
        try:
            with io.BytesIO(pdf_data) as pdf_stream:
                with pdfplumber.open(pdf_stream) as pdf:
                    return len(pdf.pages)
        except:
            return 0
    
    def _get_page_count_pypdf2(self, pdf_data: bytes) -> int:
        """Get page count using PyPDF2"""
        try:
            with io.BytesIO(pdf_data) as pdf_stream:
                pdf_reader = PyPDF2.PdfReader(pdf_stream)
                return len(pdf_reader.pages)
        except:
            return 0
    
    def create_document_record(self, filename: str, pdf_data: bytes, 
                             upload_datetime: datetime) -> PDFDocument:
        """Create a PDF document record"""
        # Extract content
        extraction_result = self.extract_text_content(pdf_data)
        content = extraction_result["content"]
        page_count = extraction_result["page_count"]
        
        # Generate file hash
        content_hash = hashlib.sha256(pdf_data).hexdigest()
        
        # Extract title from filename or content
        title = extract_title_from_content(content) or filename.replace('.pdf', '')
        
        # Generate unique file ID
        file_id = f"pdf_{content_hash[:16]}_{int(upload_datetime)}"
        
        return PDFDocument(
            file_id=file_id,
            datetime=get_current_vietnam_timestamp(),
            upload_datetime=upload_datetime,
            title=title,
            content=content,
            source_file=filename,
            file_size=len(pdf_data),
            content_hash=content_hash,
            page_count=page_count,
            version=1
        )
    
    def validate_pdf(self, pdf_data: bytes) -> tuple[bool, Optional[str]]:
        """Validate if the data is a valid PDF"""
        try:
            with io.BytesIO(pdf_data) as pdf_stream:
                PyPDF2.PdfReader(pdf_stream)
            return True, None
        except Exception as e:
            return False, f"Invalid PDF file: {str(e)}"


# Singleton instance
pdf_service = PDFService()