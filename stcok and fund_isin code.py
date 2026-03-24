# -*- coding: utf-8 -*-
import re
import os
import sys
import time
import html
import urllib.parse
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WDM = True
except ImportError:
    USE_WDM = False


# =========================================
# 共用路徑設定
# =========================================
GOOGLE_URL = "https://www.google.com/"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FOLDER = BASE_DIR / "output_data"
OUTPUT_FILE = OUTPUT_FOLDER / "ISIN Code.xlsx"

# 左邊程式用的輸入檔
INPUT_FILE = BASE_DIR / "No ISIN Code.xlsx"

# 右邊程式用的 MoneyDJ 設定
START_URL = "https://tbbfws.moneydj.com/W4/wb/wb01.djhtm?a=JFZ17-0101&aspid=TBB"
TARGET_FUNDS = {
    "0101": "JFZ17-0101",
    "0138": "JFZ23-0138"
}
GOOGLE_FUND_CODE = "0125"
GOOGLE_QUERY_TEXT = f"{GOOGLE_FUND_CODE} 基金 isin code"

# 左邊程式參數（改快一點，接近右邊）
SEARCH_RESULT_TIMEOUT = 1.0
SEARCH_RESULT_POLL = 0.10
ROW_SLEEP_SECONDS = 0.25

ISIN_REGEX = r'\b([A-Z]{2}[A-Z0-9]{9}\d)\b'

GOOGLE_RESULT_LABEL_PATTERNS = [
    r'^\s*ISIN\s*$',
    r'^\s*ISIN\s*Code\s*$',
    r'^\s*ISIN\s*代碼\s*$',
    r'^\s*ISIN代碼\s*$',
    r'^\s*ISIN\s*編碼\s*$',
    r'^\s*ISIN編碼\s*$',
    r'^\s*ISIN\s*碼\s*$',
    r'^\s*國際證券辨識碼\s*$',
    r'^\s*國際證券識別碼\s*$',
]

GOOGLE_RESULT_LABEL_REGEXES = [
    r'<strong[^>]*>\s*ISIN\s*Code\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*ISIN\s*代碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*ISIN代碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*ISIN\s*編碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*ISIN編碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*ISIN\s*碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*國際證券辨識碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*國際證券識別碼\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',
    r'<strong[^>]*>\s*ISIN\s*[:：]?\s*</strong>\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)',

    r'\bISIN\s*Code\b[^.\n]{0,300}?\bis\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\bISIN\s*Code\b[^.\n]{0,300}?\bwas\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\bISIN\s*Code\b[^。\.\n]{0,300}?為\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

    r'\bISIN\b[^.\n]{0,300}?\bis\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\bISIN\b[^.\n]{0,300}?\bwas\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\bISIN\b[^。\.\n]{0,300}?為\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

    r'\bISIN\s*碼\b[^.\n]{0,300}?\bis\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\bISIN\s*碼\b[^.\n]{0,300}?\bwas\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\bISIN\s*碼\b[^。\.\n]{0,300}?為\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

    r'\b國際證券(?:識別|辨識)碼\b[^.\n]{0,300}?\bis\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\b國際證券(?:識別|辨識)碼\b[^.\n]{0,300}?\bwas\b\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\b國際證券(?:識別|辨識)碼\b[^。\.\n]{0,300}?為\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

    r'\buses\s+ISIN\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    r'\busing\s+ISIN\s*(?:<strong[^>]*>)?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
]


def ensure_output_folder():
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-features=Translate,BackForwardCache")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.page_load_strategy = "eager"

    # options.add_argument("--headless=new")

    if USE_WDM:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(20)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def clean_html_for_regex(page_html: str) -> str:
    if not page_html:
        return ""
    text = html.unescape(page_html)
    text = text.replace("\xa0", " ")
    text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def is_numeric_alpha_symbol(value) -> bool:
    if value is None:
        return False
    text = str(value).strip().upper()
    if not text:
        return False
    if re.fullmatch(r'\d+\s*[A-Z]{1,5}', text):
        return True
    if re.fullmatch(r'\d+[.\-][A-Z]{1,5}', text):
        return True
    return False


def extract_isin_from_text_patterns(text: str):
    if not text:
        return None

    text = normalize_text(text)

    patterns = [
        r'\bISIN\s*Code\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*代碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN代碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*編碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN編碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\b國際證券辨識碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\b國際證券識別碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

        r'\bISIN\s*Code\b[^.\n]{0,300}?\bis\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*Code\b[^.\n]{0,300}?\bwas\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*Code\b[^。\.\n]{0,300}?為\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

        r'\bISIN\b[^.\n]{0,300}?\bis\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\b[^.\n]{0,300}?\bwas\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\b[^。\.\n]{0,300}?為\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

        r'\bISIN\s*碼\b[^.\n]{0,300}?\bis\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*碼\b[^.\n]{0,300}?\bwas\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\s*碼\b[^。\.\n]{0,300}?為\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

        r'\b國際證券(?:識別|辨識)碼\b[^.\n]{0,300}?\bis\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\b國際證券(?:識別|辨識)碼\b[^.\n]{0,300}?\bwas\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\b國際證券(?:識別|辨識)碼\b[^。\.\n]{0,300}?為\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',

        r'\buses\s+ISIN\s+([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\busing\s+ISIN\s+([A-Z]{2}[A-Z0-9]{9}\d)\b',

        r'\bISIN\b.{0,150}?\bis\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\b.{0,150}?\bwas\b\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
        r'\bISIN\b.{0,150}?為\s*([A-Z]{2}[A-Z0-9]{9}\d)\b',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).upper()
    return None


def extract_isin_from_html(page_html: str):
    if not page_html:
        return None

    soup = BeautifulSoup(page_html, "html.parser")
    label_tags = soup.find_all(["strong", "b"])

    for tag in label_tags:
        label_text = normalize_text(tag.get_text(" ", strip=True))
        label_text = re.sub(r'[:：]\s*$', '', label_text).strip()

        matched_label = any(
            re.match(pattern, label_text, flags=re.IGNORECASE)
            for pattern in GOOGLE_RESULT_LABEL_PATTERNS
        )
        if not matched_label:
            continue

        collected_text = []
        for sib in tag.next_siblings:
            if isinstance(sib, str):
                text = sib
            else:
                text = sib.get_text(" ", strip=True)

            text = normalize_text(text)
            if text:
                collected_text.append(text)

            joined = " ".join(collected_text)
            match = re.search(ISIN_REGEX, joined, flags=re.IGNORECASE)
            if match:
                return match.group(1).upper()

        if tag.parent:
            parent_text = normalize_text(tag.parent.get_text(" ", strip=True))
            parent_text = re.sub(
                r'^\s*(ISIN\s*Code|ISIN\s*代碼|ISIN代碼|ISIN\s*編碼|ISIN編碼|ISIN\s*碼|國際證券辨識碼|國際證券識別碼|ISIN)\s*[:：]?\s*',
                '',
                parent_text,
                flags=re.IGNORECASE
            )
            match = re.search(ISIN_REGEX, parent_text, flags=re.IGNORECASE)
            if match:
                return match.group(1).upper()

    full_text = normalize_text(soup.get_text(" ", strip=True))
    isin_code = extract_isin_from_text_patterns(full_text)
    if isin_code:
        return isin_code

    raw_html = clean_html_for_regex(page_html)
    for pattern in GOOGLE_RESULT_LABEL_REGEXES:
        match = re.search(pattern, raw_html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).upper()

    return None


def wait_for_search_result_html(driver, timeout=SEARCH_RESULT_TIMEOUT, poll_interval=SEARCH_RESULT_POLL):
    end_time = time.time() + timeout
    last_html = ""
    while time.time() < end_time:
        page_html = driver.page_source
        last_html = page_html
        page_text = clean_html_for_regex(page_html)

        for pattern in GOOGLE_RESULT_LABEL_REGEXES:
            if re.search(pattern, page_html, flags=re.IGNORECASE | re.DOTALL):
                return page_html
            if re.search(pattern, page_text, flags=re.IGNORECASE | re.DOTALL):
                return page_html

        time.sleep(poll_interval)

    return last_html if last_html else driver.page_source


def google_search_left(driver, query: str):
    search_url = "https://www.google.com/search?q=" + urllib.parse.quote(query)
    driver.get(search_url)

    try:
        WebDriverWait(driver, 2.2).until(
            lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"]
        )
    except Exception:
        pass

    time.sleep(1)

    return wait_for_search_result_html(driver)


def get_first_sheet_df(file_path):
    xls = pd.ExcelFile(file_path)
    first_sheet_name = xls.sheet_names[0]
    df = pd.read_excel(file_path, sheet_name=first_sheet_name)
    return df, first_sheet_name


def find_stock_column(df: pd.DataFrame):
    columns_lower_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in ["stock", "ticker", "symbol", "name"]:
        if candidate in columns_lower_map:
            return columns_lower_map[candidate]
    return df.columns[0]


def find_existing_isin_column(df: pd.DataFrame):
    columns_lower_map = {str(col).strip().lower(): col for col in df.columns}
    candidate_names = [
        "isin", "isin code", "isin_code", "isin碼", "isin 碼",
        "isin代碼", "isin 代碼", "isin編碼", "isin 編碼",
        "國際證券辨識碼", "國際證券識別碼",
    ]
    for candidate in candidate_names:
        if candidate in columns_lower_map:
            return columns_lower_map[candidate]
    return None


def get_existing_isin_from_row(row, existing_isin_col):
    if not existing_isin_col:
        return None
    value = row.get(existing_isin_col, None)
    if pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    match = re.search(ISIN_REGEX, text, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def build_query(row_index_zero_based: int, row, first_col_name, stock_col_name):
    if row_index_zero_based in [0, 1]:
        base_value = row[first_col_name]
        if pd.isna(base_value):
            return None
        return f"{str(base_value).strip()} isin code"
    else:
        stock_value = row[stock_col_name]
        if pd.isna(stock_value) or str(stock_value).strip() == "":
            return None
        return f"{str(stock_value).strip()} stock isin code"


def build_stock_fallback_query(row, stock_col_name):
    stock_value = row[stock_col_name]
    if pd.isna(stock_value):
        return None
    stock_text = str(stock_value).strip()
    if not stock_text:
        return None
    if is_numeric_alpha_symbol(stock_text):
        return None
    return f"{stock_text} isin code"


def try_extract_isin_with_fallback(driver, primary_query, fallback_query=None):
    page_html = google_search_left(driver, primary_query)
    isin_code = extract_isin_from_html(page_html)
    if isin_code:
        return isin_code, primary_query, "成功"

    if fallback_query and fallback_query.strip():
        if fallback_query.strip().lower() != primary_query.strip().lower():
            page_html = google_search_left(driver, fallback_query)
            isin_code = extract_isin_from_html(page_html)
            if isin_code:
                return isin_code, fallback_query, "成功(備援查詢)"

    return None, primary_query, "未找到"


def drop_unwanted_columns(df: pd.DataFrame):
    drop_cols = ["Google_Search_Query", "Search_Status"]
    existing_drop_cols = [col for col in drop_cols if col in df.columns]
    if existing_drop_cols:
        df = df.drop(columns=existing_drop_cols)
    return df


def run_left_sheet():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"找不到檔案：{INPUT_FILE}")

    df, _ = get_first_sheet_df(INPUT_FILE)
    if df.empty:
        raise ValueError("Excel 第一個工作表沒有資料。")

    first_col_name = df.columns[0]
    stock_col_name = find_stock_column(df)
    existing_isin_col = find_existing_isin_column(df)

    if "Google_Search_Query" not in df.columns:
        df["Google_Search_Query"] = ""
    if "ISIN_Code" not in df.columns:
        df["ISIN_Code"] = ""
    if "Search_Status" not in df.columns:
        df["Search_Status"] = ""

    driver = setup_driver()
    try:
        for idx, row in df.iterrows():
            try:
                existing_isin = get_existing_isin_from_row(row, existing_isin_col)
                if existing_isin:
                    df.at[idx, "ISIN_Code"] = existing_isin
                    df.at[idx, "Search_Status"] = "沿用原欄位ISIN"
                    continue

                primary_query = build_query(idx, row, first_col_name, stock_col_name)
                fallback_query = build_stock_fallback_query(row, stock_col_name)

                if not primary_query:
                    df.at[idx, "Search_Status"] = "查無搜尋關鍵字"
                    continue

                isin_code, final_query_used, status_text = try_extract_isin_with_fallback(
                    driver=driver,
                    primary_query=primary_query,
                    fallback_query=fallback_query
                )

                df.at[idx, "Google_Search_Query"] = final_query_used

                if isin_code:
                    df.at[idx, "ISIN_Code"] = isin_code
                    df.at[idx, "Search_Status"] = status_text
                else:
                    df.at[idx, "Search_Status"] = "未找到"

                if "stock isin code" in primary_query.lower():
                    time.sleep(ROW_SLEEP_SECONDS + 2)
                else:
                    time.sleep(ROW_SLEEP_SECONDS)

            except Exception as e:
                df.at[idx, "Search_Status"] = f"失敗: {str(e)}"

    finally:
        driver.quit()

    df = drop_unwanted_columns(df)
    return df


def click_basic_info(driver, wait):
    basic_info_link = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[.//span[contains(normalize-space(),'基本資料')]]")
        )
    )
    driver.execute_script("arguments[0].click();", basic_info_link)


def extract_isin_code_moneydj(driver):
    try:
        isin_td = driver.find_element(
            By.XPATH,
            "//td[normalize-space()='ISIN Code']/following-sibling::td[1]"
        )
        return isin_td.text.strip()
    except Exception:
        return ""


def select_fund(driver, wait, fund_value):
    select_element = wait.until(
        EC.presence_of_element_located((By.NAME, "selFund3"))
    )
    Select(select_element).select_by_value(fund_value)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(1.5)


def search_google_right(driver, wait, query):
    search_url = "https://www.google.com/search?q=" + urllib.parse.quote(query)
    driver.get(search_url)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(3.5)


def extract_isin_code_google(driver, wait):
    xpaths = [
        "//span[contains(@class,'T286Pc')]//strong[contains(normalize-space(),'ISIN 代碼')]/following::strong[1]",
        "//span[contains(@class,'T286Pc')]//strong[contains(normalize-space(),'ISIN Code')]/following::strong[1]",
        "//strong[contains(normalize-space(),'ISIN 代碼')]/following::strong[1]",
        "//strong[contains(normalize-space(),'ISIN Code')]/following::strong[1]",
        "//*[contains(normalize-space(),'ISIN 代碼')]",
        "//*[contains(normalize-space(),'ISIN Code')]",
        "//*[contains(normalize-space(),'ISIN')]"
    ]

    for xp in xpaths:
        try:
            elem = wait.until(EC.presence_of_element_located((By.XPATH, xp)))
            text = elem.text.strip()

            if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", text):
                return text

            m = re.search(r"([A-Z]{2}[A-Z0-9]{10})", text)
            if m:
                return m.group(1)
        except Exception:
            pass

    try:
        html_text = driver.page_source
        patterns = [
            r"ISIN\s*代碼\s*[:：]?\s*</strong>\s*<strong[^>]*>\s*([A-Z]{2}[A-Z0-9]{10})",
            r"ISIN\s*Code\s*[:：]?\s*</strong>\s*<strong[^>]*>\s*([A-Z]{2}[A-Z0-9]{10})",
            r"ISIN\s*(?:代碼|Code)\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{10})"
        ]
        for pattern in patterns:
            m = re.search(pattern, html_text, re.I)
            if m:
                return m.group(1).strip()
    except Exception:
        pass

    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        patterns = [
            r"ISIN\s*代碼\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{10})",
            r"ISIN\s*Code\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{10})",
            r"ISIN\s*(?:代碼|Code)\s*[:：]?\s*([A-Z]{2}[A-Z0-9]{10})"
        ]
        for pattern in patterns:
            m = re.search(pattern, body_text, re.I)
            if m:
                return m.group(1).strip()
    except Exception:
        pass

    return ""


def run_right_sheet():
    driver = setup_driver()
    wait = WebDriverWait(driver, 15)

    try:
        results = []

        driver.get(START_URL)
        time.sleep(2)

        for fund_code, fund_value in TARGET_FUNDS.items():
            driver.get(START_URL)
            time.sleep(2)

            select_fund(driver, wait, fund_value)
            click_basic_info(driver, wait)
            time.sleep(1.5)

            isin_code = extract_isin_code_moneydj(driver)
            results.append({
                "Fund": fund_code,
                "ISIN Code": isin_code
            })

        search_google_right(driver, wait, GOOGLE_QUERY_TEXT)
        google_isin = extract_isin_code_google(driver, wait)

        results.append({
            "Fund": GOOGLE_FUND_CODE,
            "ISIN Code": google_isin
        })

        return pd.DataFrame(results, columns=["Fund", "ISIN Code"])

    finally:
        driver.quit()


def main():
    ensure_output_folder()

    print(f"程式所在資料夾：{BASE_DIR}")
    print(f"輸出資料夾：{OUTPUT_FOLDER}")
    print(f"輸出檔案：{OUTPUT_FILE}")

    sheet1_df = run_left_sheet()
    sheet2_df = run_right_sheet()

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        sheet1_df.to_excel(writer, sheet_name="工作表1", index=False)
        sheet2_df.to_excel(writer, sheet_name="工作表2", index=False)

    print(f"\n完成，已輸出：{OUTPUT_FILE}")


if __name__ == "__main__":
    main()
