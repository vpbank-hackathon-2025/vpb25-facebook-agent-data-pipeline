from google import genai

from settings.config import settings

class GeminiClientService:
    def __init__(self):
        self.gemini_client = genai.Client(api_key=settings.gemini_api_key)
