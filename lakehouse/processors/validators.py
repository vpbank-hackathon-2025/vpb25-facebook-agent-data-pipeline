import io
from typing import Tuple
import PyPDF2
from fastapi import HTTPException, status

from logs import logger
from settings.config import settings


class FileValidator:
    """Handles file validation operations"""
    
    def __init__(self):
        self.max_file_size_bytes = settings.max_file_size_mb * 1024 * 1024
    
    def validate_file_size(self, file_content: bytes, filename: str):
        """Validate file size against configured limits"""
        if len(file_content) > self.max_file_size_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File {filename} has size exceeds {settings.max_file_size_mb}MB limit"
            )
        
        if len(file_content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is empty"
            )
    
    def validate_pdf_content(self, file_content: bytes):
        """Validate PDF file content"""
        try:
            # Try to read PDF with PyPDF2
            pdf_stream = io.BytesIO(file_content)
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            
            # Check if PDF has pages
            if len(pdf_reader.pages) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="PDF file has no pages"
                )
            
            
        except Exception as e:
            logger.error(f"PDF validation failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid PDF file format"
            )
    
    def validate_txt_content(self, file_content: bytes):
        """Validate TXT file content"""
        try:
            # Try to decode with various encodings
            text_content = None
            successful_encoding = None
            
            for encoding in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
                try:
                    text_content = file_content.decode(encoding)
                    successful_encoding = encoding
                    break
                except UnicodeDecodeError:
                    continue
            
            if text_content is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Could not decode text file with supported encodings"
                )
            
            # Check if file has content
            if len(text_content.strip()) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Text file is empty or contains only whitespace"
                )
            
            # Check for minimum content length if configured
            if hasattr(settings, 'min_txt_content_length'):
                if len(text_content.strip()) < settings.min_txt_content_length:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Text content too short (minimum {settings.min_txt_content_length} characters)"
                    )
            
            logger.info(f"TXT validation successful, encoding: {successful_encoding}, length: {len(text_content)}")
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"TXT validation failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid text file format"
            )
    
    def validate_filename(self, filename: str, file_type: str):
        """Validate filename format and characters"""
        if not filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename cannot be empty"
            )
        
        # Check for invalid characters
        invalid_chars = ['<', '>', ':', '"', '|', '?', '*', '\\', '/']
        if any(char in filename for char in invalid_chars):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Filename contains invalid characters: {invalid_chars}"
            )
        
        # Check filename length
        if len(filename) > 255:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename too long (maximum 255 characters)"
            )
        
        # Check extension matches file type
        expected_extension = f".{file_type.lower()}"
        if not filename.lower().endswith(expected_extension):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Filename must end with {expected_extension}"
            )


# Factory function for dependency injection
def create_file_validator() -> FileValidator:
    """Create FileValidator instance"""
    return FileValidator()