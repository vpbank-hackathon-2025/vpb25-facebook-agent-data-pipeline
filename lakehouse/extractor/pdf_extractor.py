import os
import uuid
import sys

from google.genai import types
import pathlib
import httpx

from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TesseractCliOcrOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption

from datetime import datetime

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from logs import logger
from api.models.schemas import PDFDocument
from lakehouse.storage.file_manager import file_manager
from settings.config import settings
from ai_services.gemini_client import GeminiClientService

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

            # Summarize PDF content using Gemini
            try:
                logger.info(f"Summarizing PDF content using Gemini for file: {filename}")
                summarize_details = self.summarize_details_pdf_content_using_gemini(local_path)
            except Exception as e:
                logger.error(f"Error summarizing PDF content: {e}")
                summarize_details = None

            # Extract content from PDF file
            extracted_content = self.extract_pdf_content_using_docling(local_path)

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
                summarize_details=summarize_details,
                content=f"{result.bucket_name}/{result.object_name}",
                source_file=filename,
                file_size=file_info["size"],
                page_count=extracted_content.get("page_count", 1),
                version=1,
            )

            return {"document": document, "extracted_content": extracted_content}

        except Exception as e:
            logger.error(f"Error creating PDF document record: {e}")
            raise Exception(f"Failed to create PDF document: {e}")

    def summarize_details_pdf_content_using_gemini(self, file_path: str) -> dict:
        """Summarize text content from PDF using Gemini"""
        try:
            client = GeminiClientService().gemini_client
            filepath = pathlib.Path(file_path)

            prompt = """I am working on RAG system, summarize the file for easy search later. 
            Response only content with same language as file and # markdown format, not include 'Here is a summary of the provided document, structured for easy retrieval in a RAG system:' or something like that. 
            Just response content only."""

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    types.Part.from_bytes(
                        data=filepath.read_bytes(),
                        mime_type='application/pdf',
                    ),
                    prompt
                ]
            )
            return response.text
        except Exception as e:
            logger.error(f"Error extracting PDF content: {e}")
            raise Exception(f"PDF extraction failed: {e}")
    
    def extract_pdf_content_using_docling(self, local_path: str) -> dict:
        """Extract text content from PDF"""
        try:
            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = True
            pipeline_options.do_table_structure = True
            pipeline_options.table_structure_options.do_cell_matching = True
            pipeline_options.ocr_options.lang = ["vi"]
            
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

if __name__ == "__main__":
    # Example usage
    extractor = PDFExtractor()
    summarize_details = extractor.summarize_details_pdf_content_using_gemini("temp/website-the-le-uu-dai-tinh-nang-danh-cho-chu-the-tin-dung-vpbank-z-jcb.pdf")
    print(summarize_details)