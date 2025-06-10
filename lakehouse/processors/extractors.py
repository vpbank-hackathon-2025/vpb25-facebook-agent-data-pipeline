import hashlib
import uuid
from datetime import datetime
from typing import Union

from logs import logger
from api.models.schemas import PDFDocument, TXTDocument


class PDFExtractor:
    """Handles PDF content extraction and document creation"""
    
    def create_document_record(self, filename: str, file_content: bytes, 
                             upload_datetime: datetime) -> PDFDocument:
        """Create PDF document record from file content"""
        try:
            # Extract content (implement your PDF extraction logic)
            extracted_content = self._extract_pdf_content(file_content)
            
            # Calculate content hash
            content_hash = hashlib.md5(file_content).hexdigest()
            
            # Create document record
            document = PDFDocument(
                file_id=str(uuid.uuid4()),
                datetime=upload_datetime,
                upload_datetime=upload_datetime,
                title=self._extract_title_from_filename(filename),
                content=extracted_content["text"],
                source_file=filename,
                file_size=len(file_content),
                content_hash=content_hash,
                page_count=extracted_content.get("page_count", 1),
                version=1
            )
            
            return document
            
        except Exception as e:
            logger.error(f"Error creating PDF document record: {e}")
            raise Exception(f"Failed to create PDF document: {e}")
    
    def _extract_pdf_content(self, file_content: bytes) -> dict:
        """Extract text content from PDF"""
        try:
            # Implement your PDF extraction logic here
            # This is a placeholder - replace with actual PDF extraction
            import PyPDF2
            import io
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            
            return {
                "text": text.strip(),
                "page_count": len(pdf_reader.pages)
            }
            
        except Exception as e:
            logger.error(f"Error extracting PDF content: {e}")
            raise Exception(f"PDF extraction failed: {e}")
    
    def _extract_title_from_filename(self, filename: str) -> str:
        """Extract title from filename"""
        # Remove extension and replace underscores/hyphens with spaces
        title = filename.rsplit('.', 1)[0]
        title = title.replace('_', ' ').replace('-', ' ')
        return title.title()


class TXTExtractor:
    """Handles TXT content extraction and document creation"""
    
    def create_document_record(self, filename: str, file_content: bytes,
                             upload_datetime: datetime) -> TXTDocument:
        """Create TXT document record from file content"""
        try:
            # Extract content
            text_content = self._extract_txt_content(file_content)
            
            # Calculate content hash
            content_hash = hashlib.md5(file_content).hexdigest()
            
            # Create document record
            document = TXTDocument(
                file_id=str(uuid.uuid4()),
                datetime=upload_datetime,
                upload_datetime=upload_datetime,
                title=self._extract_title_from_filename(filename),
                content=text_content,
                source_file=filename,
                source_link=None,  # Could be populated from metadata
                file_size=len(file_content),
                content_hash=content_hash,
                version=1
            )
            
            return document
            
        except Exception as e:
            logger.error(f"Error creating TXT document record: {e}")
            raise Exception(f"Failed to create TXT document: {e}")
    
    def _extract_txt_content(self, file_content: bytes) -> str:
        """Extract text content from TXT file"""
        try:
            # Try different encodings
            for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                try:
                    return file_content.decode(encoding)
                except UnicodeDecodeError:
                    continue
            
            # If all encodings fail, use utf-8 with error handling
            return file_content.decode('utf-8', errors='replace')
            
        except Exception as e:
            logger.error(f"Error extracting TXT content: {e}")
            raise Exception(f"TXT extraction failed: {e}")
    
    def _extract_title_from_filename(self, filename: str) -> str:
        """Extract title from filename"""
        # Remove extension and replace underscores/hyphens with spaces
        title = filename.rsplit('.', 1)[0]
        title = title.replace('_', ' ').replace('-', ' ')
        return title.title()