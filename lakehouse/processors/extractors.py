import hashlib
import os
import uuid

# from docling.document_converter import DocumentConverter
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TesseractCliOcrOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption

from datetime import datetime
from typing import Union

from logs import logger
from api.models.schemas import PDFDocument, TXTDocument
from lakehouse.storage.file_manager import file_manager
from settings.config import settings

class PDFExtractor:
    """Handles PDF content extraction and document creation"""
    
    def create_document_record(
            self, 
            file_path: str,
            upload_datetime: datetime,
        ) -> dict:
        """Create PDF document record from file content"""
        try:
            filename = file_path.split('/')[-1]
            local_path = f"temp/{filename}"
            
            # Download file from staging
            file_manager.download_file_to_local(settings.staging_bucket, file_path, local_path)
            file_info = file_manager.get_file_info(settings.staging_bucket, file_path)

            # Extract content from PDF file
            extracted_content = self._extract_pdf_content(local_path)

            local_md_path = f"temp/{extracted_content['doc_name']}.md"
            # save content to file
            with open(local_md_path, "w") as f:
                f.write(extracted_content["text"])

            # upload content to s3
            result = file_manager.upload_file_from_local(
                bucket=settings.silver_bucket,
                object_name=f"pdfs/{extracted_content['doc_name']}.md",
                local_path=local_md_path
            )

            # delete local file
            os.remove(local_path)
            os.remove(local_md_path)
            
            # Create document record
            document = PDFDocument(
                file_id=str(uuid.uuid4().hex),
                upload_datetime=upload_datetime,
                title=self._extract_title_from_filename(filename),
                content=f"{result.bucket_name}/{result.object_name}",
                source_file=filename,
                file_size=file_info["size"],
                page_count=extracted_content.get("page_count", 1),
                version=1,
            )
            
            return document
            
        except Exception as e:
            logger.error(f"Error creating PDF document record: {e}")
            raise Exception(f"Failed to create PDF document: {e}")
    
    def _extract_pdf_content(self, local_path: str) -> dict:
        """Extract text content from PDF"""
        try:
            # converter = DocumentConverter()
            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = True
            pipeline_options.do_table_structure = True
            pipeline_options.table_structure_options.do_cell_matching = True
            pipeline_options.ocr_options.lang = ["vi"]

            ocr_options = TesseractCliOcrOptions(lang=["auto"])
            
            pipeline_options.accelerator_options = AcceleratorOptions(
                num_threads=4, device=AcceleratorDevice.AUTO
            )

            doc_converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
                }
            )
            doc_result = doc_converter.convert(local_path).document
            page_count = len(doc_result.pages)

            md_content = doc_result.export_to_markdown()
            
            return {
                "text": md_content,
                "page_count": page_count,
                "doc_name": doc_result.name
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