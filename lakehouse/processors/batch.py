import asyncio
from typing import List, Dict, Any
from fastapi import HTTPException, status

from lakehouse.processors.document_processor import DocumentProcessor
from lakehouse.storage.file_manager import file_manager
from settings.config import settings
from logs import logger
from api.models.schemas import ProcessingStatus


class BatchProcessor:
    """Handles batch processing of files"""
    
    def __init__(self):
        self.document_processor = DocumentProcessor()
        self.staging_bucket = settings.staging_bucket
    
    async def process_all_files(self) -> Dict[str, Any]:
        """Process all files in staging bucket"""
        try:
            # Get all files
            pdf_files = file_manager.list_files(self.staging_bucket, "pdfs/")
            txt_files = file_manager.list_files(self.staging_bucket, "txts/")
            
            # Process files concurrently
            pdf_results = await self._process_files_batch(pdf_files, "pdf")
            txt_results = await self._process_files_batch(txt_files, "txt")
            
            # Combine results
            all_results = pdf_results + txt_results
            success_count = sum(1 for r in all_results if r.status == "completed")
            error_count = len(all_results) - success_count
            
            return {
                "processed_files": all_results,
                "total_processed": len(all_results),
                "success_count": success_count,
                "error_count": error_count,
                "pdf_count": len(pdf_results),
                "txt_count": len(txt_results)
            }
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Batch processing failed: {e}"
            )
    
    async def process_pdf_files(self) -> Dict[str, Any]:
        """Process only PDF files"""
        try:
            pdf_files = file_manager.list_files(self.staging_bucket, "pdfs/")
            results = await self._process_files_batch(pdf_files, "pdf")
            
            success_count = sum(1 for r in results if r.status == "completed")
            
            return {
                "processed_files": results,
                "total_processed": len(results),
                "success_count": success_count,
                "error_count": len(results) - success_count
            }
            
        except Exception as e:
            logger.error(f"Error processing PDF batch: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"PDF batch processing failed: {e}"
            )
    
    async def process_txt_files(self) -> Dict[str, Any]:
        """Process only TXT files"""
        try:
            txt_files = file_manager.list_files(self.staging_bucket, "txts/")
            results = await self._process_files_batch(txt_files, "txt")
            
            success_count = sum(1 for r in results if r.status == "completed")
            
            return {
                "processed_files": results,
                "total_processed": len(results),
                "success_count": success_count,
                "error_count": len(results) - success_count
            }
            
        except Exception as e:
            logger.error(f"Error processing TXT batch: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"TXT batch processing failed: {e}"
            )
    
    async def _process_files_batch(self, files: List[Dict[str, Any]], 
                                  file_type: str) -> List[ProcessingStatus]:
        """Process a batch of files concurrently"""
        if not files:
            return []
        
        # Extract file names from list response
        file_paths = [f["name"] if isinstance(f, dict) else f for f in files]
        
        # Create tasks for concurrent processing
        tasks = []
        for file_path in file_paths:
            if file_type == "pdf":
                task = self.document_processor.process_pdf_file(file_path)
            else:
                task = self.document_processor.process_txt_file(file_path)
            tasks.append(task)
        
        # Execute tasks concurrently with limited concurrency
        semaphore = asyncio.Semaphore(settings.max_concurrent_processing)
        
        async def bounded_task(task):
            async with semaphore:
                return await task
        
        bounded_tasks = [bounded_task(task) for task in tasks]
        results = await asyncio.gather(*bounded_tasks, return_exceptions=True)
        
        # Handle exceptions in results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error processing file {file_paths[i]}: {result}")
                processed_results.append(ProcessingStatus(
                    file_id="",
                    filename=file_paths[i].split('/')[-1],
                    status="failed",
                    message=f"Processing error: {str(result)}"
                ))
            else:
                processed_results.append(result)
        
        return processed_results


# Factory function for dependency injection
def create_batch_processor() -> BatchProcessor:
    """Create BatchProcessor instance"""
    return BatchProcessor()