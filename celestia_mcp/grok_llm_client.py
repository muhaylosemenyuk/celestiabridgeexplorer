import aiohttp
from config import GROK_API_KEY, GROK_API_BASE_URL, GROK_MODEL

async def llm_client(prompt: str) -> str:
    url = f"{GROK_API_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROK_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": GROK_MODEL,
        "messages": [
            {"role": "system", "content": "You are an assistant for analyzing Celestia blockchain data. Respond in the user's query language. Give short and clear answers."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "stream": False,
        "reasoning": False,
        "top_p": 0.9
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data, timeout=30) as resp:
            resp.raise_for_status()
            result = await resp.json()
            if 'choices' not in result or not result['choices']:
                raise Exception("Grok API returned empty response")
            message = result['choices'][0]['message']
            content = message.get('content', '')
            if content and content.strip():
                return content.strip()
            raise Exception("Grok API returned empty content")
