from flask import Flask, render_template, request
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import concurrent.futures
import os
import time
import threading
import logging

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

        df = crawl_data(url)


        if df is not None and not df.empty:
            y, m, d = day.split('-')
            folder_path = f"./tables/{gold_type}/{y}/{m}"

            os.makedirs(folder_path, exist_ok=True)
            df.to_csv(f"{folder_path}/{d}.csv", index=False)
        else:
            print(f"Khong co du lieu cho ngay {day}")
    except Exception as e:
        print(f"{e} Khong co du lieu cho ngay {day}")


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

@app.route("/api/start-crawl", methods=["POST"])
def start_crawl():
    data = request.json
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    gold_type = data.get("gold_type")

    if not all([start_date, end_date, gold_type]):
        logging.error("Yêu cầu thiếu start_date, end_date, hoặc gold_type")
        return {
            "status": "error",
            "message": "Yêu cầu không hợp lệ. Cần cung cấp đủ start_date, end_date, và gold_type"
        }, 400

    thread = threading.Thread(target=run_heavy_task, args=(start_date, end_date, gold_type))
    thread.start()

    return {"status": "accepted", "message": "Crawl task started in background"}
    
def run_heavy_task(start, end, gold_type):
    # URL cố định của webhook trên n8n
    # Container 'crawlgoldapp' có thể gọi container 'n8n' qua tên dịch vụ
    webhook_url = "http://n8n:5678/webhook/crawl-finished"

    try:
        multi_thread(startDate=start, endDate=end, gold_type=gold_type)

        success_payload = {
            "status": "success",
            "message": f"Đã cào xong vàng {gold_type} từ {start} đến {end}",
        }
        try:
            requests.post(webhook_url, json=success_payload, timeout=10)
            logging.info(f"Successfully sent success callback to {webhook_url}")
        except requests.RequestException as e:
            logging.error(f"Failed to send success callback to n8n: {e}")

    except Exception as e:
        logging.error(f"An error occurred during the crawl task: {e}")

        error_payload = {
            "status": "error",
            "message": str(e)
        }
        try:
            requests.post(webhook_url, json=error_payload, timeout=10)
            logging.info(f"Successfully sent error callback to {webhook_url}")
        except requests.RequestException as callback_error:
            logging.error(f"Failed to send error callback to n8n: {callback_error}")

@app.route("/", methods=["GET"])
def hello():
    return "Hello Roy"
 
if __name__ == '__main__':
    # multi_thread(startDate="2015-03-01",endDate="2025-03-31", gold_type="sjc")
    # print(os.cpu_count())
    app.run(host="0.0.0.0", port=5000)