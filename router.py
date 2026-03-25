from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from ollama import Client
import os
import requests
import json
import uvicorn
from datetime import datetime
import logging

app = FastAPI(title="Gold CRAWL API", version="1.0.0")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RouteRequest(BaseModel):
    text: str
    callback_url: Optional[str] = None
    chat_id: str


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
Bạn là một trợ lý ảo phân luồng dữ liệu thông minh. Nhiệm vụ của bạn là đọc tin nhắn và trích xuất thành ĐÚNG MỘT object JSON. 
Hôm nay là ngày {today_str}.

QUY TẮC PHÂN LOẠI (Dựa trên động từ và mục đích):

1. CAO_VANG: Khi người dùng muốn thực hiện hành động thu thập dữ liệu từ web. 
   - Từ khóa: "cào", "quét", "crawl", "lấy dữ liệu mới", "update giá".
   - Bất kể có thời gian hay không, cứ muốn "Cào/Quét" là vào đây.
   - JSON: {{"intent": "CAO_VANG", "payload": {{"user_input": "{text}", "gold_type": "<sjc, nhẫn...>"}}}}

2. NAP_DATA: Khi người dùng muốn đưa dữ liệu vào hệ thống lưu trữ/database.
   - Từ khóa: "nạp", "lưu", "import", "ghi vào máy".
   - Yêu cầu: Bạn phải tính toán start_date và end_date (YYYY-MM-DD) dựa trên thời gian user nhắc tới.
   - Nếu không đề cập loại vàng, mặc định "sjc".
   - JSON: {{"intent": "NAP_DATA", "payload": {{"start_date": "<YYYY-MM-DD>", "end_date": "<YYYY-MM-DD>", "gold_type": "<sjc hoặc loại khác>"}}}}

3. PHAN_TICH: Khi người dùng muốn đặt câu hỏi hoặc xem nhận định về dữ liệu đã có.
   - Từ khóa: "phân tích", "hỏi", "xem", "tổng hợp", "so sánh", "dự báo".
   - JSON: {{"intent": "PHAN_TICH", "payload": {{"question": "{text}", "gold_type": "..."}}}}

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
    logger.info(f"Received request for chat_id: {req.chat_id} with text: '{req.text}', callback_url: {req.callback_url}")
    try:
        result = get_intent_from_ollama(req.text)
        intent = result.get("intent")
        payload = result.get("payload", {})
        payload["chat_id"] = req.chat_id
        
        # Ensure gold_type is lowercase for consistency
        if "gold_type" in payload:
            payload["gold_type"] = payload["gold_type"].lower()


        logging.info(payload)
        logger.info(f"Intent classified as: {intent}")
    
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