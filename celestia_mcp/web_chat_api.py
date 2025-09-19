import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("celestia_mcp.web_chat_api")

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from celestia_mcp.mcp_server import CelestiaMCP
from celestia_mcp.core.llm_router import get_llm_client

app = FastAPI()
llm_client = get_llm_client()
mcp = CelestiaMCP(llm_client, local_api_url="http://localhost:8002")

@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    user_message = data.get("message", "")
    user_id = data.get("user_id", "default")
    logger.info(f"Web Chat API - Received message from user {user_id}: {user_message}")
    response = await mcp.call_tool("consult_celestia", user_message, user_id=user_id)
    return JSONResponse({"response": response})

@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <html>
    <head>
        <title>CelestiaBridge Chat</title>
        <style>
            body { font-family: sans-serif; max-width: 600px; margin: 40px auto; }
            #history { border: 1px solid #ccc; padding: 10px; min-height: 200px; margin-bottom: 10px; }
            textarea { width: 100%; height: 60px; }
            button { padding: 8px 16px; }
        </style>
    </head>
    <body>
        <h2>CelestiaBridge AI Chat</h2>
        <div id="history"></div>
        <textarea id="msg" placeholder="Type your message..."></textarea><br>
        <button onclick="sendMsg()">Send</button>
        <script>
            let history = [];
            async function sendMsg() {
                let msg = document.getElementById('msg').value;
                if (!msg) return;
                document.getElementById('msg').value = '';
                history.push('<b>You:</b> ' + msg);
                updateHistory();
                let resp = await fetch('/chat', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({message: msg, user_id: 'testuser'})
                });
                let data = await resp.json();
                history.push('<b>AI:</b> ' + data.response);
                updateHistory();
            }
            function updateHistory() {
                document.getElementById('history').innerHTML = history.join('<br><br>');
            }
            // Додаємо відправку через Enter (Shift+Enter — новий рядок)
            document.getElementById('msg').addEventListener('keydown', function(e) {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    sendMsg();
                }
            });
        </script>
    </body>
    </html>
    """
