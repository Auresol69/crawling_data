from flask import Flask, render_template, request, g
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import concurrent.futures
import os
import time
import threading
import logging
from ollama import Client
from functools import wraps
import json
from datetime import date

# Cấu hình cơ bản
logging.basicConfig(
    level=logging.DEBUG, # Mức độ thấp nhất được ghi lại
    format='%(asctime)s - %(levelname)s - %(message)s' # Định dạng dòng log
)

app = Flask(__name__)

def crawl_data(url="https://giavang.org/trong-nuoc/sjc/lich-su/2010-09-15.html", parser_type="lxml"): 
    try:
        response = requests.get(url, timeout=15)
    except requests.RequestException:
        return None

    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text, parser_type)
    tables = soup.find_all("table")
    if len(tables) < 1:
        return None

    all_dataframes = []
    for table in tables:
        table_html = str(table)
        try:
            df = pd.read_html(io.StringIO(table_html))[0]
            all_dataframes.append(df)
        except ValueError:
            return None

    if not all_dataframes:
        return None

    all_dataframes[1] = all_dataframes[1].rename(columns={'Thời gian cập nhật': 'Thời gian'})

    df = pd.concat(all_dataframes, ignore_index=True)

    required_columns = ["Khu vực", "Loại vàng", "Mua vào", "Bán ra", "Thời gian"]
    if any(col not in df.columns for col in required_columns):
        return None

    df = df.dropna(subset=["Mua vào"])

    # ~df["Cai gi cung duoc"]
    df = df[~df["Loại vàng"].astype(str).str.contains("http", na=False)]

    df["Khu vực"] = df["Khu vực"].ffill()
    df["Loại vàng"] = df["Loại vàng"].ffill()

    df["Mua vào"] = pd.to_numeric(
        df["Mua vào"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df["Bán ra"] = pd.to_numeric(
        df["Bán ra"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    
    df["Thời gian"] = pd.to_datetime(
        df["Thời gian"].astype(str).str.strip(), format="%H:%M:%S %d/%m/%Y", errors="coerce"
    ).dt.strftime("%d/%m/%Y")
    
    df.loc[~df['Loại vàng'].str.contains('pnj|sjc', case=False, na=False), 'Loại vàng'] = 'PNJ'

    df.loc[df['Khu vực'].astype(str).str.contains("Giá vàng nữ trang", na = False), 'Khu vực'] = 'TPHCM'

    return df


def single_day(day,gold_type):
    try:
        url = f"https://giavang.org/trong-nuoc/{gold_type}/lich-su/{day}.html"
        logging.info(f"Fetching data from {url}")

        df = crawl_data(url)

        if df is not None and not df.empty:
            logging.info(f"Got data for {day}, shape: {df.shape}")
            y, m, d = day.split('-')
            folder_path = f"/app/tables/{gold_type}/{y}/{m}"
            
            logging.info(f"Creating folder: {folder_path}")
            os.makedirs(folder_path, exist_ok=True)
            
            file_path = f"{folder_path}/{d}.csv"
            logging.info(f"Saving to: {file_path}")
            df.to_csv(file_path, index=False)
            logging.info(f"Successfully saved {day}")
        else:
            logging.warning(f"Khong co du lieu cho ngay {day}")
    except Exception as e:
        logging.error(f"Error for {day}: {e}", exc_info=True)


def multi_thread(gold_type, startDate, endDate):
    start_time = time.time()
    data_list = pd.date_range(start=startDate,end=endDate, freq='D')
    list_str_days = data_list.strftime('%Y-%m-%d').tolist()


    # Để ThreadPoolExecutor tự quyết định số luồng tối ưu.
    # Đây là tác vụ I/O-bound (chờ mạng), không phải CPU-bound,
    # nên GPU không giúp tăng tốc.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for day in list_str_days:
            executor.submit(single_day, day, gold_type)
    end_time = time.time()

    print(f"Thoi gian crawl data: {end_time - start_time} giay")

        

# @app.route("/", methods=["GET", "POST"])
# def index():
#     tables = None
#     error = None

#     if request.method == "POST":
#         url = request.form.get("url", "").strip()
#         parser = request.form.get("parser", "lxml")
#         if not url:
#             error = "Ban chua nhap URL hop le. Vi du: https://example.com"
#         else:
#             df = crawl_data(url=url, parser_type=parser)
#             if df is None:
#                 error = "Khong lay duoc bang du lieu. Hay kiem tra URL, parser, hoac ket noi mang."
#             else:
#                 tables = [df.to_html(index=False, border=0)]
#                 df.to_csv("PipelineScraping/tables/gia_vang.csv",index=False)
#     return render_template("index.html", tables=tables, error=error)

def call_ollama(prompt, model="llama3"): 
    client = Client(host='http://ollama:11434')

    response = client.generate(
        model=model,
        prompt=prompt,
        format="json",
        stream=False
    )
    
    return(response['response'])

# Middleware || Decorator
def ai_intent_parser(f):
    @wraps(f) # f = start_crawl()
    # *args : Không xác định được số lượng phần tử truyền vào vd tinh_tong(1, 2, 3)
    # **kwargs: Giúp truyền các tham số theo kiểu key=value giống Dictionary vd gioi_thieu(ten="Bao", nganh="Software Engineering")
    def decorated_function(*args, **kwargs):
        data = request.get_json(force=True, silent=True)
        user_input = data.get("user_input")
        # Solve được việc user gửi kèm dấu ""
        clean_input = str(user_input).replace('"', "'")

        if user_input:
            today = date.today()

            instruction = f"""
            Bạn là một trợ lý trích xuất dữ liệu. Hãy phân tích yêu cầu của người dùng về việc cào giá vàng.
            Yêu cầu:
            1. Trả về JSON với 3 key: "start_date", "end_date", "gold_type".
            2. Định dạng ngày là YYYY-MM-DD. 
            3. Nếu người dùng nói "hôm nay", hãy dùng ngày {today}.
            4. "gold_type" chỉ được là "sjc" hoặc "pnj".
            5. QUAN TRỌNG: Nếu người dùng nhắn tin không liên quan hoặc thiếu thông tin nào, hãy để giá trị đó là null.

            User message: "{clean_input}"
            """

            raw_ai_out = call_ollama(prompt=instruction)
            
            if not raw_ai_out or not raw_ai_out.strip():
                logging.error("AI trả về chuỗi rỗng")
                return {
                    "status": "error",
                    "message": "AI trả về chuỗi rỗng"
                }, 500
            
            try:
                parsed = json.loads(raw_ai_out)
            except json.JSONDecodeError as e:
                logging.error(f"AI trả về JSON không hợp lệ: {raw_ai_out[:200]}")
                return {
                    "status": "error",
                    "message": f"AI trả về JSON không hợp lệ: {str(e)}"
                }, 500
            
            # Nếu chỉ có start_date, set end_date = start_date
            if parsed.get("start_date") and not parsed.get("end_date"):
                parsed["end_date"] = parsed["start_date"]
                            
            # Tiêm dữ liệu vào request object để route phía sau sử dụng
            data.update({
                "start_date": parsed.get("start_date"),
                "end_date": parsed.get("end_date"),
                "gold_type": parsed.get("gold_type")
            })
        
        # Lấy dữ liệu từ data (đã được cập nhật từ AI hoặc từ request)
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        gold_type = data.get("gold_type")
        callback_url = data.get("callback_url")

        if not all([start_date, end_date, gold_type]):
            logging.error("Yêu cầu thiếu start_date, end_date, hoặc gold_type")
            return {
                "status": "error",
                "message": "Yêu cầu không hợp lệ. Cần cung cấp đủ start_date, end_date, và gold_type"
            }, 400
        
        # Lưu dữ liệu vào Flask's g object để hàm start_crawl có thể truy cập
        g.start_date = start_date
        g.end_date = end_date
        g.gold_type = gold_type
        g.callback_url = callback_url
        
        return f(*args, **kwargs)
    return decorated_function

@app.route("/api/start-crawl", methods=["POST"])
@ai_intent_parser # Khi gọi route này, Python sẽ chạy qua Decorator
def start_crawl():
    # Lấy dữ liệu từ Flask's g object (được set bởi decorator)
    start_date = g.start_date
    end_date = g.end_date
    gold_type = g.gold_type
    callback_url = g.callback_url

    thread = threading.Thread(target=run_heavy_task, args=(start_date, end_date, gold_type, callback_url))
    thread.start()

    return {"status": "accepted", "message": "Crawl task started in background"}
    
def run_heavy_task(start, end, gold_type, callback_url=None):
    # Nếu callback_url không được cung cấp, sử dụng URL mặc định
    if not callback_url:
        callback_url = "http://n8n:5678/webhook/crawl-finished"

    try:
        multi_thread(startDate=start, endDate=end, gold_type=gold_type)

        success_payload = {
            "status": "success",
            "message": f"Đã cào xong vàng {gold_type} từ {start} đến {end}",
        }
        try:
            requests.post(callback_url, json=success_payload, timeout=10)
            logging.info(f"Successfully sent success callback to {callback_url}")
        except requests.RequestException as e:
            logging.error(f"Failed to send success callback: {e}")

    except Exception as e:
        logging.error(f"An error occurred during the crawl task: {e}")

        error_payload = {
            "status": "error",
            "message": str(e)
        }
        try:
            requests.post(callback_url, json=error_payload, timeout=10)
            logging.info(f"Successfully sent error callback to {callback_url}")
        except requests.RequestException as callback_error:
            logging.error(f"Failed to send error callback: {callback_error}")

@app.route("/", methods=["GET"])
def hello():
    return "Hello Roy"
 
if __name__ == '__main__':
    # multi_thread(startDate="2015-03-01",endDate="2025-03-31", gold_type="sjc")
    # print(os.cpu_count())
    app.run(host="0.0.0.0", port=5000)