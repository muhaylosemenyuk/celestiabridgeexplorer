import os
from google import genai

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

class GeminiLLMClient:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY is not set")
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model = "gemini-2.5-flash-lite"

    async def __call__(self, prompt: str) -> str:
        print("GeminiLLMClient called")
        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt
        )
        return response.text
