# -*- coding: utf-8 -*-
import multiprocessing
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pdfplumber
import io
import urllib3
import time
import os
import sys
import numpy as np
import calendar
import re
from datetime import datetime, timedelta

# Selenium 相關
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from smtplib import SMTP

# MIME 組信
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header


def is_month_end(check_date=None):
    """判斷是否為月底（保留原函式，不影響其他流程）"""
    if check_date is None:
        check_date = datetime.now()

    last_day = calendar.monthrange(check_date.year, check_date.month)[1]
    return check_date.day == last_day


def keep_output_columns(df):
    """輸出檔案只保留 商品代號、ISIN Code 兩欄"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["商品代號", "ISIN Code"])

    df = df.copy()

    if "商品代號" not in df.columns:
        df["商品代號"] = ""
    if "ISIN Code" not in df.columns:
        df["ISIN Code"] = ""

    return df[["商品代號", "ISIN Code"]].copy()


def clean_dataframe_fully(df):
    """清理資料：去空格、處理空值、並確保以 ISIN 為主進行去重"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["商品代號", "ISIN Code"])

    df = df.copy()
    df = df.astype(str).apply(lambda x: x.str.strip())
    df.replace(['nan', 'None', '', 'NaN'], np.nan, inplace=True)

    if "ISIN Code" in df.columns:
        df["ISIN Code"] = df["ISIN Code"].astype(str).str.strip()
        df = df.dropna(subset=["ISIN Code"])
        df = df[df["ISIN Code"].astype(str).str.strip() != ""]

        if "商品代號" not in df.columns:
            df["商品代號"] = ""

        df["商品代號"] = df["商品代號"].fillna("").astype(str).str.strip()

        # 讓有商品代號的優先保留
        df["has_code"] = df["商品代號"].apply(lambda x: 0 if x else 1)
        df = df.sort_values(by=["has_code", "商品代號"], na_position="last")
        df = df.drop_duplicates(subset=["ISIN Code"], keep="first")
        df = df.drop(columns=["has_code"], errors="ignore")

    return df.reset_index(drop=True)


def format_product_code(code):
    """統一商品代號格式"""
    if pd.isna(code) or str(code).strip() == "":
        return ""
    clean_code = str(code).replace(" US", "").strip()
    return f"{clean_code} US"


def build_total_collection_csv(df_ice_cleaned, df_new_missing, output_path):
    """
    產出總集合 CSV
    內容包含：
    1. ICE PM 基準檔（已剔除查無）
    2. 本月比對後新增標的
    但輸出只保留：
    - 商品代號
    - ISIN Code
    """
    frames = []

    if df_ice_cleaned is not None and not df_ice_cleaned.empty:
        frames.append(df_ice_cleaned.copy())

    if df_new_missing is not None and not df_new_missing.empty:
        frames.append(df_new_missing.copy())

    if frames:
        output_df = pd.concat(frames, ignore_index=True, sort=False)
    else:
        output_df = pd.DataFrame(columns=["商品代號", "ISIN Code"])

    if "商品代號" not in output_df.columns:
        output_df["商品代號"] = ""
    if "ISIN Code" not in output_df.columns:
        output_df["ISIN Code"] = ""

    output_df["商品代號"] = output_df["商品代號"].apply(format_product_code)
    output_df = clean_dataframe_fully(output_df)
    output_df = keep_output_columns(output_df)

    output_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✓ 總集合CSV檔案已產生：{output_path}")
    print(f"✓ 總集合筆數（ICE + 本月新增）：{len(output_df)}")

    return output_df


def build_new_items_csv(df_new_missing, output_path):
    """
    產出本月新增標的 CSV
    只保留：
    - 商品代號
    - ISIN Code
    """
    if df_new_missing is None or df_new_missing.empty:
        output_df = pd.DataFrame(columns=["商品代號", "ISIN Code"])
    else:
        output_df = df_new_missing.copy()

    if "商品代號" not in output_df.columns:
        output_df["商品代號"] = ""
    if "ISIN Code" not in output_df.columns:
        output_df["ISIN Code"] = ""

    output_df["商品代號"] = output_df["商品代號"].apply(format_product_code)
    output_df = clean_dataframe_fully(output_df)
    output_df = keep_output_columns(output_df)

    output_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✓ 本月新增標的CSV已產生：{output_path}")
    print(f"✓ 本月新增標的筆數：{len(output_df)}")

    return output_df


def extract_yyyymm_from_filename(filename):
    """
    從檔名抓 YYYY年_MM月
    例如：
    2026年_03月_總集合.csv -> (2026, 3)
    """
    m = re.search(r"(\d{4})年_(\d{2})月_總集合\.csv$", filename)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def find_previous_total_collection_file(target_folder, current_filename):
    """
    找出目前這份總集合之前的最近一份總集合檔
    """
    all_files = []
    for f in os.listdir(target_folder):
        if f.endswith("_總集合.csv") and f != current_filename:
            ym = extract_yyyymm_from_filename(f)
            if ym:
                all_files.append((ym[0], ym[1], f))

    if not all_files:
        return None

    all_files.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return os.path.join(target_folder, all_files[0][2])


def compare_total_collections(current_total_df, current_total_path, target_folder, current_month_prefix):
    """
    比對『這次總集合』與『上一次總集合』
    只產出：
    1. 本次新增於總集合.csv
    """
    current_filename = os.path.basename(current_total_path)
    previous_total_path = find_previous_total_collection_file(target_folder, current_filename)

    result = {
        "previous_total_path": previous_total_path,
        "new_in_total_path": None,
        "new_in_total_count": 0
    }

    if previous_total_path is None or not os.path.exists(previous_total_path):
        print("⚠ 找不到上一份總集合，略過總集合比對。")
        return result

    try:
        df_prev = pd.read_csv(previous_total_path, encoding="utf-8-sig")
        df_prev = clean_dataframe_fully(df_prev)
        df_prev = keep_output_columns(df_prev)

        df_curr = clean_dataframe_fully(current_total_df)
        df_curr = keep_output_columns(df_curr)

        prev_isin_set = set(df_prev["ISIN Code"].astype(str).str.strip())
        curr_isin_set = set(df_curr["ISIN Code"].astype(str).str.strip())

        # 本次新增
        df_new_in_total = df_curr[df_curr["ISIN Code"].isin(curr_isin_set - prev_isin_set)].copy()
        df_new_in_total = keep_output_columns(df_new_in_total)

        new_in_total_path = os.path.join(target_folder, f"{current_month_prefix}_總集合比上次新增.csv")
        df_new_in_total.to_csv(new_in_total_path, index=False, encoding="utf-8-sig")

        print("\n--- 總集合與上次總集合比對完成 ---")
        print(f"上一份總集合：{previous_total_path}")
        print(f"本次新增於總集合：{len(df_new_in_total)} 筆 -> {new_in_total_path}")

        result.update({
            "previous_total_path": previous_total_path,
            "new_in_total_path": new_in_total_path,
            "new_in_total_count": len(df_new_in_total)
        })
        return result

    except Exception as e:
        print(f"✗ 總集合比對失敗: {e}")
        return result


def fetch_kgi_data(final_data):
    """抓取凱基證券資料"""
    try:
        url = "https://www.kgi.com.tw/zh-tw/product-market/news-and-announcement/announcement/announcement-detail?id=b68f7bba98ee46ebb1ff66f7a7175714"
        resp = requests.get(url, verify=False, timeout=20)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")

        if table:
            for tr in table.find_all("tr"):
                cols = tr.find_all("td")
                if len(cols) >= 3:
                    code = cols[1].get_text(strip=True)
                    isin = cols[2].get_text(strip=True)
                    if code != "商品代號" and isin:
                        final_data.append({"商品代號": code, "ISIN Code": isin})

        print("✓ 凱基證券 抓取成功")
    except Exception as e:
        print(f"✗ 凱基證券 錯誤: {e}")


def fetch_fubon_data(final_data):
    """抓取富邦證券PDF資料"""
    try:
        pdf_url = "https://www.fbs.com.tw/wcm/new_web/trade/trade_20221104_545388/20260102PTP.pdf"
        resp = requests.get(pdf_url, verify=False, timeout=20)
        resp.raise_for_status()

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    for row in table:
                        if not row or "代號" in str(row):
                            continue

                        cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                        if len(cleaned_row) >= 4 and cleaned_row[3]:
                            final_data.append({
                                "商品代號": cleaned_row[2],
                                "ISIN Code": cleaned_row[3]
                            })

        print("✓ 富邦證券 (PDF) 抓取成功")
    except Exception as e:
        print(f"✗ 富邦證券 錯誤: {e}")


def fetch_psc_data(final_data):
    """抓取統一證券資料"""
    try:
        url = "https://www.pscnet.com.tw/pscnetForeign/menuContent.do?main_id=46293fac1e000000675191dd21f81854&sub_id=462940cb3c000000137692530067d3a8&id=58cec3e6e9000000672b2ec87c690e25"
        resp = requests.get(url, verify=False, timeout=20)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        tbody = soup.find("tbody")

        if tbody:
            rows = tbody.find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) >= 4:
                    code = cols[2].get_text(strip=True)
                    isin = cols[3].get_text(strip=True)
                    if isin:
                        final_data.append({"商品代號": code, "ISIN Code": isin})

        print("✓ 統一證券 抓取成功")
    except Exception as e:
        print(f"✗ 統一證券 錯誤: {e}")


def fetch_sinopac_data(final_data):
    """抓取永豐金證券資料（改用較穩定的抓法）"""
    print("正在啟動瀏覽器抓取永豐金 (開啟實體瀏覽器 + 自動翻頁抓取商品代號)...")
    driver = None

    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # 降低被判定為自動化
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver_path = ChromeDriverManager().install()
        if not driver_path.endswith(".exe") and os.name == "nt":
            driver_path = os.path.join(os.path.dirname(driver_path), "chromedriver.exe")

        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)

        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """
            }
        )

        driver.get("https://www.sinopacasia.com/tc/marketing/market/1")
        wait = WebDriverWait(driver, 25)

        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ant-table-tbody")))
        time.sleep(3)

        page_count = 1
        seen_signatures = set()

        while True:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ant-table-tbody")))
            time.sleep(2)

            table = driver.find_element(By.CLASS_NAME, "ant-table-tbody")
            rows = table.find_elements(By.TAG_NAME, "tr")

            if not rows:
                print("  - 本頁沒有資料列，停止翻頁")
                break

            current_page_data_count = 0
            current_page_rows = []

            for row in rows:
                tds = row.find_elements(By.TAG_NAME, "td")
                if len(tds) >= 3:
                    code = tds[0].text.strip()
                    isin = tds[2].text.strip()

                    current_page_rows.append((code, isin))

                    if isin:
                        final_data.append({
                            "商品代號": code,
                            "ISIN Code": isin
                        })
                        current_page_data_count += 1

            page_signature = tuple(current_page_rows)
            if page_signature in seen_signatures:
                print("  - 偵測到重複頁面資料，停止翻頁")
                break
            seen_signatures.add(page_signature)

            print(f"  - 永豐金第 {page_count} 頁：抓取 {current_page_data_count} 筆")

            try:
                next_btn = wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "li.ant-pagination-next:not(.ant-pagination-disabled)")
                    )
                )

                old_first_row = rows[0].text.strip()

                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_btn)

                def page_changed(d):
                    try:
                        new_table = d.find_element(By.CLASS_NAME, "ant-table-tbody")
                        new_rows = new_table.find_elements(By.TAG_NAME, "tr")
                        if not new_rows:
                            return False
                        new_first_row = new_rows[0].text.strip()
                        return new_first_row != old_first_row
                    except Exception:
                        return False

                wait.until(page_changed)
                page_count += 1
                time.sleep(2)

            except Exception:
                break

        print(f"✓ 永豐金證券 抓取成功，共計 {page_count} 頁")

    except Exception as e:
        print(f"✗ 永豐金證券 錯誤: {e}")
    finally:
        if driver:
            driver.quit()


def load_ice_pm_file(base_dir, target_folder):
    """讀取 ICE PM 基準檔，並剔除查無標的"""
    print("\n--- 步驟 2：讀取 ICE PM 資料 ---")

    ice_name = "匯整_ICE_PM_清理後.csv"
    path_in_output = os.path.join(target_folder, ice_name)
    path_in_root = os.path.join(base_dir, ice_name)
    ice_file_path = path_in_output if os.path.exists(path_in_output) else path_in_root

    try:
        df_ice = pd.read_csv(ice_file_path, encoding="utf-8-sig")
        df_ice = df_ice.astype(str).apply(lambda x: x.str.strip())
        df_ice.replace(['nan', 'None', '', 'NaN'], np.nan, inplace=True)

        if "ISIN Code" not in df_ice.columns:
            raise ValueError("ICE PM 檔案缺少 'ISIN Code' 欄位")

        if "商品代號" not in df_ice.columns:
            df_ice["商品代號"] = ""

        df_ice["ISIN Code"] = df_ice["ISIN Code"].astype(str).str.strip()
        df_ice = df_ice.dropna(subset=["ISIN Code"])
        df_ice = df_ice[df_ice["ISIN Code"].astype(str).str.strip() != ""]
        df_ice = df_ice.drop_duplicates(subset=["ISIN Code"])

        if len(df_ice.columns) < 3:
            raise ValueError("ICE PM 檔案欄位不足，無法用第3欄判斷查無")

        target_col_name = df_ice.columns[2]
        df_ice_cleaned = df_ice[
            ~df_ice[target_col_name].astype(str).str.contains("查無", na=False)
        ].copy()

        df_ice_cleaned = clean_dataframe_fully(df_ice_cleaned)

        print("✓ ICE PM 基準檔載入成功")
        print(f"  - 原始筆數：{len(df_ice)}")
        print(f"  - 剔除查無後筆數：{len(df_ice_cleaned)}")

        return df_ice_cleaned

    except Exception as e:
        print(f"✗ 基準檔讀取失敗: {e}")
        return pd.DataFrame(columns=["商品代號", "ISIN Code"])


def run_task():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    final_data = []

    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    target_folder = os.path.join(base_dir, "output_data")
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        print(f"已建立輸出資料夾：{target_folder}")

    today = datetime.now()
    current_month_prefix = today.strftime("%Y年_%m月")
    last_month_date = today.replace(day=1) - timedelta(days=1)
    last_month_prefix = last_month_date.strftime("%Y年_%m月")

    current_month_new_csv_filename = f"{current_month_prefix}_新增標的.csv"
    last_month_filename = f"{last_month_prefix}_差異標的.csv"
    recurring_filename = f"{current_month_prefix}_與上月重複標的.csv"
    total_collection_filename = f"{current_month_prefix}_總集合.csv"

    print(f"--- 步驟 1：開始抓取原始資料 (系統時間：{today.strftime('%Y-%m-%d %H:%M:%S')}) ---")

    fetch_kgi_data(final_data)
    fetch_fubon_data(final_data)
    fetch_psc_data(final_data)
    fetch_sinopac_data(final_data)

    df_raw_all = pd.DataFrame(final_data)
    df_raw_all = clean_dataframe_fully(df_raw_all)
    print(f"✓ 四大券商整理後總筆數：{len(df_raw_all)}")

    df_ice_cleaned = load_ice_pm_file(base_dir, target_folder)
    ice_count = len(df_ice_cleaned)

    print("\n--- 步驟 3：進行本月比對 ---")
    missing_df = df_raw_all[
        ~df_raw_all["ISIN Code"].isin(df_ice_cleaned["ISIN Code"])
    ].copy()

    if "商品代號" not in missing_df.columns:
        missing_df["商品代號"] = ""

    missing_df["商品代號"] = missing_df["商品代號"].apply(format_product_code)
    missing_df = clean_dataframe_fully(missing_df)
    missing_df = keep_output_columns(missing_df)

    new_count = len(missing_df)

    total_collection_path = os.path.join(target_folder, total_collection_filename)
    new_items_csv_path = os.path.join(target_folder, current_month_new_csv_filename)

    # 每次執行都先產總集合
    df_total_collection = build_total_collection_csv(
        df_ice_cleaned=df_ice_cleaned,
        df_new_missing=missing_df,
        output_path=total_collection_path
    )
    total_collection_count = len(df_total_collection)

    # 每次執行都產本月新增標的 CSV
    build_new_items_csv(
        df_new_missing=missing_df,
        output_path=new_items_csv_path
    )

    # 每次執行都比對這次總集合 vs 上一次總集合
    total_compare_result = compare_total_collections(
        current_total_df=df_total_collection,
        current_total_path=total_collection_path,
        target_folder=target_folder,
        current_month_prefix=current_month_prefix
    )

    print("\n=== 本次結果統計 ===")
    print(f"ICE 清理後筆數：{ice_count}")
    print(f"比對後新增筆數：{new_count}")
    print(f"總集合筆數：{total_collection_count}")
    print(f"總集合比上次新增：{total_compare_result['new_in_total_count']}")

    print("\n--- 步驟 5：跨月比對 ---")
    last_month_file_path = os.path.join(target_folder, last_month_filename)
    if os.path.exists(last_month_file_path):
        try:
            df_last_month = pd.read_csv(last_month_file_path, encoding="utf-8-sig")
            df_last_month["ISIN Code"] = df_last_month["ISIN Code"].astype(str).str.strip()

            recurring_df = missing_df[
                missing_df["ISIN Code"].isin(df_last_month["ISIN Code"])
            ].copy()

            recurring_df = keep_output_columns(recurring_df)

            if not recurring_df.empty:
                recurring_output_path = os.path.join(target_folder, recurring_filename)
                recurring_df.to_csv(recurring_output_path, index=False, encoding="utf-8-sig")
                print(f"⚠ 發現 {len(recurring_df)} 筆標的連續兩月未入庫")
        except Exception as e:
            print(f"✗ 跨月比對失敗: {e}")

    return {
        "mode": "standard",
        "new_items_csv_path": new_items_csv_path,
        "total_collection_path": total_collection_path,
        "total_collection_count": total_collection_count,
        "new_count": new_count,
        "compare_result": total_compare_result
    }


def attach_file_for_outlook(msg, file_path):
    """避免 Outlook / Exchange 中文檔名附件異常"""
    filename = os.path.basename(file_path)

    with open(file_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())

    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=Header(filename, "utf-8").encode()
    )

    part.add_header(
        "Content-Type",
        "application/octet-stream",
        name=Header(filename, "utf-8").encode()
    )

    msg.attach(part)


def send_monthly_report():
    result = run_task()

    total_collection_path = result["total_collection_path"]
    new_items_csv_path = result.get("new_items_csv_path")
    total_collection_count = result["total_collection_count"]
    new_count = result["new_count"]
    compare_result = result.get("compare_result", {})

    if not total_collection_path or not os.path.exists(total_collection_path):
        print("✗ 找不到總集合附件檔案，取消寄送。")
        return

    if not new_items_csv_path or not os.path.exists(new_items_csv_path):
        print("✗ 找不到新增標的CSV附件檔案，取消寄送。")
        return

    recipients = ["Timmy.Lee@feis.com.tw", "IT@feis.com.tw"]

    msg = MIMEMultipart()
    msg["Subject"] = Header(f"PTP 每月報表 - {datetime.now().strftime('%Y/%m')}", "utf-8")
    msg["From"] = "Timmy.Lee@feis.com.tw"
    msg["To"] = ", ".join(recipients)

    body_text = (
        f"各位好，附件為本月 PTP 總集合清單（CSV檔）與新增標的清單（CSV檔）。\n"
        f"比對後新增筆數：{new_count}\n"
        f"總集合筆數：{total_collection_count}\n"
        f"與上次總集合相比新增：{compare_result.get('new_in_total_count', 0)}"
    )
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    attach_file_for_outlook(msg, total_collection_path)
    attach_file_for_outlook(msg, new_items_csv_path)

    try:
        server = SMTP("webmail.feis.com.tw", 25)
        server.sendmail(
            from_addr=msg["From"],
            to_addrs=recipients,
            msg=msg.as_string()
        )
        server.quit()
        print("✓ 郵件寄送成功")
    except Exception as e:
        print(f"✗ 郵件寄送失敗: {e}")


if __name__ == "__main__":
    send_monthly_report()
