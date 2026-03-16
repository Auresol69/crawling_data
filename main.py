from flask import Flask, render_template, request
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import csv
import concurrent.futures
import os

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
    
    df.loc[df['Loại vàng'].astype(str).str.contains(" ", na = False), 'Loại vàng'] = 'PNJ'

    df.loc[df['Khu vực'].astype(str).str.contains("Giá vàng nữ trang", na = False), 'Khu vực'] = 'TPHCM'

    return df


def single_day(day,gold_type):
    try:
        url = f"https://giavang.org/trong-nuoc/{gold_type}/lich-su/{day}.html"

        df = crawl_data(url)


        if df is not None and not df.empty:
            y, m, d = day.split('-')
            folder_path = f"PipelineScraping/tables/{gold_type}/{y}/{m}"

            os.makedirs(folder_path, exist_ok=True)
            df.to_csv(f"{folder_path}/{d}.csv", index=False)
        else:
            print(f"Khong co du lieu cho ngay {day}")
    except Exception as e:
        print(f"{e} Khong co du lieu cho ngay {day}")


def multi_thread(gold_type, startDate="2026-03-01", endDate="2026-03-10"):
    data_list = pd.date_range(start=startDate,end=endDate, freq='D')
    list_str_days = data_list.strftime('%Y-%m-%d').tolist()


    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for day in list_str_days:
            executor.submit(single_day, day, gold_type)


        

@app.route("/", methods=["GET", "POST"])
def index():
    tables = None
    error = None

    if request.method == "POST":
        url = request.form.get("url", "").strip()
        parser = request.form.get("parser", "lxml")
        if not url:
            error = "Ban chua nhap URL hop le. Vi du: https://example.com"
        else:
            df = crawl_data(url=url, parser_type=parser)
            if df is None:
                error = "Khong lay duoc bang du lieu. Hay kiem tra URL, parser, hoac ket noi mang."
            else:
                tables = [df.to_html(index=False, border=0)]
                df.to_csv("PipelineScraping/tables/gia_vang.csv",index=False)
                

    return render_template("index.html", tables=tables, error=error)

if __name__ == '__main__':
    multi_thread(startDate="2010-03-01",endDate="2010-03-31", gold_type="sjc")
    app.run(host="0.0.0.0", port=5000, debug=True)