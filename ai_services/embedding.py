import time
from google import genai
from google.genai import types
from typing import List

from settings.config import settings
from logs import logger
from ai_services.gemini_client import GeminiClientService

class GeminiEmbeddingService:
    def __init__(self):
        self.client = GeminiClientService().gemini_client
        
    def get_embedding(self, text: str, model: str = "text-embedding-004") -> List[float] | None:
        try:
            response = self.client.models.embed_content(
                    model=model,
                    contents=text,
                    config=types.EmbedContentConfig(task_type="SEMANTIC_SIMILARITY")
            )
            return response.embeddings[0].values
        except Exception as e:
            logger.error(f"Error getting embedding: {e}")
            return None

    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        embeddings = []
        for text in texts:
            embedding = self.get_embedding(text)
            embeddings.append(embedding)
            time.sleep(0.3)  # Rate limiting
        return embeddings
