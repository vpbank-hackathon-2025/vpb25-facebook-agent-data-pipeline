from typing import Optional

from lakehouse.storage.file_manager import file_manager
from lakehouse.storage.writers import iceberg_writer
from lakehouse.processors.extractors import PDFExtractor, TXTExtractor
from settings.config import settings
from logs import logger
from utils.helper import get_current_vietnam_timestamp
from api.models.schemas import ProcessingStatus


class DocumentProcessor:
    
    def __init__(self):
        self.pdf_extractor = PDFExtractor()
        self.txt_extractor = TXTExtractor()
        self.staging_bucket = settings.staging_bucket
    
    async def process_pdf_file(self, file_path: str) -> ProcessingStatus:
        """Process individual PDF file"""
        try:
            # Extract and create document record
            upload_datetime = get_current_vietnam_timestamp()
            document = self.pdf_extractor.create_document_record(file_path, upload_datetime)
            
            # Insert into Iceberg table
            success = iceberg_writer.insert_pdf_documents([document])
            
            if success:
                # Optionally move to processed folder or delete
                await self._post_process_file(file_path, "pdf")
                
                return ProcessingStatus(
                    file_id=document.file_id,
                    filename=document.source_file,
                    status="completed",
                    message="PDF processed successfully",
                    processed_at=get_current_vietnam_timestamp()
                )
            else:
                return ProcessingStatus(
                    file_id="",
                    filename=document.source_file,
                    status="failed",
                    message="Failed to insert into lakehouse"
                )
                
        except Exception as e:
            logger.error(f"Error processing PDF {file_path}: {e}")
            return ProcessingStatus(
                file_id="",
                filename=file_path.split('/')[-1],
                status="failed",
                message=f"Processing error: {str(e)}"
            )
    
    async def process_txt_file(self, file_path: str) -> ProcessingStatus:
        """Process individual TXT file"""
        try:
            filename = file_path.split('/')[-1]
            
            # Download file from staging
            file_content = file_manager.download_file(self.staging_bucket, file_path)
            
            # Extract and create document record
            upload_datetime = get_current_vietnam_timestamp()
            document = self.txt_extractor.create_document_record(
                filename, file_content, upload_datetime
            )
            
            # Insert into Iceberg table
            success = iceberg_writer.insert_txt_documents([document])
            
            if success:
                # Optionally move to processed folder or delete
                await self._post_process_file(file_path, "txt")
                
                return ProcessingStatus(
                    file_id=document.file_id,
                    filename=filename,
                    status="completed",
                    message="TXT processed successfully",
                    processed_at=get_current_vietnam_timestamp()
                )
            else:
                return ProcessingStatus(
                    file_id="",
                    filename=filename,
                    status="failed",
                    message="Failed to insert into lakehouse"
                )
                
        except Exception as e:
            logger.error(f"Error processing TXT {file_path}: {e}")
            return ProcessingStatus(
                file_id="",
                filename=file_path.split('/')[-1],
                status="failed",
                message=f"Processing error: {str(e)}"
            )
    
    async def _post_process_file(self, file_path: str, file_type: str):
        """Handle file after successful processing"""
        try:
            if settings.delete_after_processing:
                # Delete from staging
                file_manager.delete_file(self.staging_bucket, file_path)
                logger.info(f"Deleted processed file: {file_path}")
            elif settings.archive_after_processing:
                # Move to processed folder
                processed_path = f"processed/{file_type}/{file_path.split('/')[-1]}"
                file_manager.move_file(
                    self.staging_bucket, file_path,
                    self.staging_bucket, processed_path
                )
                logger.info(f"Archived processed file: {file_path} -> {processed_path}")
                
        except Exception as e:
            logger.warning(f"Error in post-processing {file_path}: {e}")
            # Don't fail the main processing for post-process errors