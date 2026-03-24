from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from ollama import Client
import os
import requests
import json
import uvicorn
from datetime import datetime

app = FastAPI(title="Gold CRAWL API", version="1.0.0")


class RouteRequest(BaseModel):
    text: str
    callback_url: Optional[str] = None


CRAWL_SERVICE_URL = os.getenv("CRAWL_SERVICE_URL", "http://crawlgoldapp:5000")
ANALYSIS_SERVICE_URL = os.getenv("ANALYSIS_SERVICE_URL", "http://rag-app:8000/ask")
INGEST_SERVICE_URL = os.getenv("INGEST_SERVICE_URL", "http://rag-app:8000/ingest")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
INTENT_MODEL = os.getenv("INTENT_MODEL", "llama3")


def get_intent_from_ollama(text: str):
    """Gọi Ollama để phân loại intent và payload, trả về dict."""

    client = Client(host=OLLAMA_HOST)

    today_str = datetime.now().strftime("%Y-%m-%d")
    prompt = f"""
Bạn là một trợ lý ảo phân luồng dữ liệu. Nhiệm vụ của bạn là đọc tin nhắn, xác định ý định (intent) và trích xuất thành ĐÚNG MỘT object JSON. KHÔNG giải thích thêm. 
Hôm nay là ngày {today_str}.

Quy tắc phân loại và cấu trúc JSON:

1. PHAN_TICH: Dùng khi người dùng có từ khóa "phân tích", "hỏi", "xem", "tổng hợp". Nếu người dùng nhắc đến ngày tháng, hãy gom nó vào câu hỏi.
{{"intent": "PHAN_TICH", "payload": {{"question": "<Câu hỏi hoàn chỉnh của user>", "gold_type": "<sjc, nhẫn, doji...>"}}}}

2. NAP_DATA: Dùng khi người dùng ra lệnh "nạp", "lấy" VÀ có đề cập đến thời gian (từ ngày nào đến ngày nào, x ngày qua, tháng trước). BẠN PHẢI TỰ TÍNH TOÁN để suy ra định dạng YYYY-MM-DD. (Ví dụ: "2 tuần qua" thì start_date là 14 ngày trước so với hôm nay).
{{"intent": "NAP_DATA", "payload": {{"start_date": "<YYYY-MM-DD>", "end_date": "<YYYY-MM-DD>", "gold_type": "<Loại vàng>"}}}}

3. CAO_VANG: CHỈ dùng khi người dùng ra lệnh cào dữ liệu chung chung mà HOÀN TOÀN KHÔNG đề cập đến bất kỳ mốc thời gian nào.
{{"intent": "CAO_VANG", "payload": {{"user_input": "<Nguyên văn câu lệnh>"}}}}

---
Tin nhắn người dùng: "{text}"
"""

    response = client.generate(
        model=INTENT_MODEL,
        prompt=prompt,
        format="json",
        stream=False
    )

    return(json.loads(response['response']))

@app.post("/router")
def route_message(req: RouteRequest):
    try:
        result = get_intent_from_ollama(req.text)
        intent = result.get("intent")
        payload = result.get("payload", {})
    
        if intent == "PHAN_TICH":
            response = requests.post(ANALYSIS_SERVICE_URL, json=payload, timeout=60)
            return response.json()
        elif intent == "NAP_DATA":
            response = requests.post(INGEST_SERVICE_URL, json=payload, timeout=60)
            return response.json()
        elif intent == "CAO_VANG":
            response = requests.post(CRAWL_SERVICE_URL, json=payload, timeout=60)
            return response.json()
        return {"status": "error", "message": "Không hiểu ý định người dùng"}
    except HTTPException as e:
        raise HTTPException(status_code=500, detail=f"Router Error: {str(e)}")

if __name__=="__main__":
    uvicorn.run("router:app", host="0.0.0.0", port=8081, reload=True)