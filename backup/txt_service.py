import hashlib
from datetime import datetime
from typing import Optional, Dict, Any
import chardet


from models.schemas import TXTDocument
from utils.text_utils import clean_text, extract_title_from_content
from logs import logger


class TXTService:
    def __init__(self):
        self.supported_encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']
    
    def extract_text_content(self, txt_data: bytes) -> str:
        """Extract text content from TXT file with encoding detection"""
        try:
            # Try to detect encoding
            detected = chardet.detect(txt_data)
            encoding = detected.get('encoding', 'utf-8')
            confidence = detected.get('confidence', 0)
            
            logger.info(f"Detected encoding: {encoding} (confidence: {confidence})")
            
            # If confidence is low, try common encodings
            if confidence < 0.7:
                content = self._try_multiple_encodings(txt_data)
            else:
                try:
                    content = txt_data.decode(encoding)
                except (UnicodeDecodeError, TypeError):
                    content = self._try_multiple_encodings(txt_data)
            
            return clean_text(content)
        
        except Exception as e:
            logger.error(f"Error extracting TXT content: {e}")
            return "[Error extracting content]"
    
    def _try_multiple_encodings(self, txt_data: bytes) -> str:
        """Try multiple encodings to decode the file"""
        for encoding in self.supported_encodings:
            try:
                content = txt_data.decode(encoding)
                logger.info(f"Successfully decoded with {encoding}")
                return content
            except (UnicodeDecodeError, TypeError):
                continue
        
        # Last resort: decode with errors='replace'
        logger.warning("Using UTF-8 with error replacement")
        return txt_data.decode('utf-8', errors='replace')
    
    def extract_source_link(self, content: str) -> Optional[str]:
        """Extract source link from content if present"""
        lines = content.split('\n')[:10]  # Check first 10 lines
        
        for line in lines:
            line = line.strip().lower()
            if any(prefix in line for prefix in ['source:', 'url:', 'link:', 'from:']):
                # Extract URL-like patterns
                import re
                url_pattern = r'https?://[^\s]+'
                matches = re.findall(url_pattern, line)
                if matches:
                    return matches[0]
        
        return None
    
    def create_document_record(self, filename: str, txt_data: bytes, 
                             upload_datetime: datetime) -> TXTDocument:
        """Create a TXT document record"""
        # Extract content
        content = self.extract_text_content(txt_data)
        
        # Generate file hash
        content_hash = hashlib.sha256(txt_data).hexdigest()
        
        # Extract title from filename or content
        title = extract_title_from_content(content) or filename.replace('.txt', '')
        
        # Extract source link if present
        source_link = self.extract_source_link(content)
        
        # Generate unique file ID
        file_id = f"txt_{content_hash[:16]}_{int(upload_datetime)}"
        
        return TXTDocument(
            file_id=file_id,
            datetime=datetime.utcnow(),
            upload_datetime=upload_datetime,
            title=title,
            content=content,
            source_file=filename,
            source_link=source_link,
            file_size=len(txt_data),
            content_hash=content_hash,
            version=1
        )
    
    def validate_txt(self, txt_data: bytes) -> tuple[bool, Optional[str]]:
        """Validate if the data is a valid text file"""
        try:
            # Try to decode the content
            self.extract_text_content(txt_data)
            
            # Check if file is not too large (basic validation)
            if len(txt_data) > 100 * 1024 * 1024:  # 100MB limit
                return False, "Text file is too large (max 100MB)"
            
            return True, None
        except Exception as e:
            return False, f"Invalid text file: {str(e)}"


# Singleton instance
txt_service = TXTService()