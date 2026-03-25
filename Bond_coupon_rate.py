# -*- coding: utf-8 -*-
import re
import time
import html
import urllib.parse
from pathlib import Path
from datetime import datetime
import os
import sys

import pandas as pd
from bs4 import BeautifulSoup, NavigableString, Tag

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WDM = True
except ImportError:
    USE_WDM = False


# =========================================
# 路徑設定：輸出到 output_data
# =========================================
def get_base_dir():
    if getattr(sys, 'frozen', False):
        return Path(os.path.dirname(sys.executable))
    return Path(os.path.dirname(os.path.abspath(__file__)))


BASE_DIR = get_base_dir()
OUTPUT_DIR = BASE_DIR / "output_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 這兩個先沿用你原本路徑
INPUT_FILE = Path(r"D:\python training\BOND_HOLDING_0313.xlsx")
INPUT_SHEET_NAME = "工作表2"
REF_FILE = Path(r"D:\python training\Bond.xlsx")

OUTPUT_FILE = OUTPUT_DIR / "BOND_SORTED_RESULT_FINAL.xlsx"

GOOGLE_URL = "https://www.google.com/"
SEARCH_RESULT_TIMEOUT = 1.2
SEARCH_RESULT_POLL = 0.10
ROW_SLEEP_SECONDS = 1.2
PAGE_SLEEP_SECONDS = 3.0


# =========================================
# 共用函式
# =========================================
def convert_coupon(rate_str):
    try:
        rate_str = str(rate_str).strip()
        if not rate_str:
            return None

        if re.fullmatch(r'\d+\s+\d+/\d+', rate_str):
            whole, frac = rate_str.split()
            num, den = frac.split('/')
            return float(whole) + float(num) / float(den)

        if re.fullmatch(r'\d+/\d+', rate_str):
            num, den = rate_str.split('/')
            return float(num) / float(den)

        if re.fullmatch(r'\d+(\.\d+)?', rate_str):
            return float(rate_str)

        return None
    except Exception:
        return None


def extract_rate_from_text(text):
    if pd.isna(text):
        return None

    text = str(text).strip()
    if not text:
        return None

    match = re.search(r'^(\d+\s+\d+/\d+)%', text)
    if match:
        return convert_coupon(match.group(1))

    match = re.search(r'^(\d+/\d+)%', text)
    if match:
        return convert_coupon(match.group(1))

    match = re.search(r'^(\d+\.?\d*)%', text)
    if match:
        return float(match.group(1))

    return None


def is_empty_coupon(x):
    if pd.isna(x):
        return True

    if isinstance(x, (int, float)):
        return False

    x = str(x).strip()
    return x == '' or x in ['None', 'nan']


COUPON_LABEL_PATTERNS = [
    r"^\s*票面利率\s*$",
    r"^\s*票面利率[:：]?\s*$",
    r"^\s*Coupon\s*Rate\s*$",
    r"^\s*Coupon\s*Rate[:：]?\s*$",
    r"^\s*票息\s*$",
    r"^\s*票息率\s*$",
]

NUM_PATTERN = r"([0-9]\s*(?:\.\s*[0-9\s]+)?)"

COUPON_TEXT_REGEXES = [
    rf'票面利率\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'Coupon\s*Rate\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'票息率\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'票息\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'with\s+a\s+coupon\s+of\s+{NUM_PATTERN}\s*%',
    rf'coupon\s+of\s+{NUM_PATTERN}\s*%',
    rf'bearing\s+interest\s+at\s+{NUM_PATTERN}\s*%',
    rf'fixed\s+rate\s+of\s+{NUM_PATTERN}\s*%',
    rf'interest\s+rate\s+of\s+{NUM_PATTERN}\s*%',
]

STRICT_HTML_REGEXES = [
    rf'票面利率\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'Coupon\s*Rate\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'票息率\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'票息\s*[:：]?\s*{NUM_PATTERN}\s*%',
    rf'with\s+a\s+coupon\s+of\s+{NUM_PATTERN}\s*%',
    rf'coupon\s+of\s+{NUM_PATTERN}\s*%',
    rf'bearing\s+interest\s+at\s+{NUM_PATTERN}\s*%',
    rf'fixed\s+rate\s+of\s+{NUM_PATTERN}\s*%',
    rf'interest\s+rate\s+of\s+{NUM_PATTERN}\s*%',
]


# =========================================
# Selenium Driver
# =========================================
def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")

    # 若要登入 profile 可自行打開
    # options.add_argument(r"--user-data-dir=C:\Users\你的帳號\AppData\Local\Google\Chrome\User Data")
    # options.add_argument("--profile-directory=Default")

    if USE_WDM:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(30)
    return driver


# =========================================
# HTML / 文字處理
# =========================================
def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = html.unescape(str(text))
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_html_for_regex(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_coupon_value(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = html.unescape(text)
    text = text.replace("%", "").replace("\\%", "")
    text = text.replace(",", "")

    m = re.search(r"([0-9]\s*(?:\.\s*[0-9\s]+)?)", text)
    if not m:
        return None

    raw = m.group(1)
    raw = re.sub(r"\s+", "", raw)

    if raw.count(".") > 1:
        return None
    if raw == ".":
        return None

    try:
        return float(raw)
    except Exception:
        return None


def is_missing_coupon(value) -> bool:
    return is_empty_coupon(value)


def find_coupon_rate_column(df: pd.DataFrame):
    columns_lower_map = {str(col).strip().lower(): col for col in df.columns}
    candidates = [
        "coupon rate", "coupon_rate", "couponrate",
        "票面利率", "票息", "票息率", "coupon"
    ]
    for c in candidates:
        if c in columns_lower_map:
            return columns_lower_map[c]

    new_col = "coupon rate"
    df[new_col] = pd.NA
    return new_col


def build_queries(row, first_col_name):
    queries = []

    a_value = row.get(first_col_name, "")
    a_text = "" if pd.isna(a_value) else str(a_value).strip()

    if not a_text:
        return queries

    queries.append(f"{a_text} bond名稱")
    return queries


# =========================================
# Google 搜尋
# =========================================
def wait_for_search_result_html(driver, timeout=SEARCH_RESULT_TIMEOUT, poll_interval=SEARCH_RESULT_POLL):
    end_time = time.time() + timeout
    last_html = ""

    while time.time() < end_time:
        page_html = driver.page_source
        last_html = page_html
        page_text = clean_html_for_regex(page_html)

        if any(re.search(p, page_text, flags=re.I | re.S) for p in STRICT_HTML_REGEXES):
            return page_html

        if "票面利率" in page_text or "Coupon Rate" in page_text or "coupon of" in page_text.lower():
            return page_html

        time.sleep(poll_interval)

    return last_html if last_html else driver.page_source


def google_search(driver, query: str):
    search_url = GOOGLE_URL + "search?q=" + urllib.parse.quote(query)
    driver.get(search_url)

    try:
        WebDriverWait(driver, 2.2).until(
            lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"]
        )
    except Exception:
        pass

    time.sleep(PAGE_SLEEP_SECONDS)
    return wait_for_search_result_html(driver)


# =========================================
# 票面利率擷取
# =========================================
def extract_coupon_from_text(text: str):
    if not text:
        return None, None

    text = normalize_text(text)

    for pattern in COUPON_TEXT_REGEXES:
        m = re.search(pattern, text, flags=re.I | re.S)
        if m:
            return normalize_coupon_value(m.group(1)), m.group(0)

    keyword_match = re.search(
        r"(票面利率|Coupon\s*Rate|票息率|票息|coupon\s+of|fixed\s+rate|interest\s+rate)",
        text,
        flags=re.I
    )
    if keyword_match:
        start = keyword_match.start()
        nearby = text[start:start + 120]

        m = re.search(r"([0-9]\s*(?:\.\s*[0-9\s]+)?)\s*%", nearby)
        if m:
            return normalize_coupon_value(m.group(1)), nearby

    return None, None


def get_nearby_value_from_parent(tag: Tag):
    parent = tag.parent
    if not parent:
        return None, None

    children = list(parent.children)

    try:
        idx = children.index(tag)
    except ValueError:
        return None, None

    nearby_parts = []
    taken = 0

    for child in children[idx + 1:]:
        if taken >= 2:
            break

        if isinstance(child, NavigableString):
            text = normalize_text(str(child))
        elif isinstance(child, Tag):
            text = normalize_text(child.get_text(" ", strip=True))
        else:
            text = ""

        if text:
            nearby_parts.append(text)
            taken += 1

    nearby_text = " ".join(nearby_parts).strip()
    if not nearby_text:
        return None, None

    m = re.search(r"^([0-9]\s*(?:\.\s*[0-9\s]+)?)\s*%", nearby_text)
    if m:
        return normalize_coupon_value(m.group(1)), nearby_text

    coupon, snippet = extract_coupon_from_text(nearby_text)
    if coupon is not None:
        return coupon, snippet if snippet else nearby_text

    return None, None


def extract_coupon_for_us94974bfn55(page_html: str, query: str = ""):
    if not page_html:
        return None, None, ""

    q = (query or "").upper()
    if "US94974BFN55" not in q:
        return None, None, ""

    raw_html = clean_html_for_regex(page_html)

    patterns = [
        r'\(\s*WFC\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*[0-9/]+\s*CORP\s*\)',
        r'WFC\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*[0-9/]+\s*CORP',
        r'富國銀行美元半年配息次順位債\s*\(\s*WFC\s*([0-9]+(?:\.[0-9]+)?)\s*%',
    ]

    for pattern in patterns:
        m = re.search(pattern, raw_html, flags=re.I | re.S)
        if m:
            return normalize_coupon_value(m.group(1)), "content", m.group(0)

    return None, None, ""


def extract_coupon_from_html(page_html: str, query: str = ""):
    if not page_html:
        return None, None, ""

    soup = BeautifulSoup(page_html, "html.parser")

    label_tags = soup.find_all(["strong", "b", "td", "th", "span", "div", "li"])
    for tag in label_tags:
        label_text = normalize_text(tag.get_text(" ", strip=True))
        label_text = re.sub(r"[:：]\s*$", "", label_text).strip()

        matched_label = any(
            re.match(pattern, label_text, flags=re.I)
            for pattern in COUPON_LABEL_PATTERNS
        )
        if not matched_label:
            continue

        coupon, snippet = get_nearby_value_from_parent(tag)
        if coupon is not None:
            return coupon, "label", f"{label_text} -> {snippet}"

    coupon, found_by, debug_snippet = extract_coupon_for_us94974bfn55(page_html, query=query)
    if coupon is not None:
        return coupon, found_by, debug_snippet

    raw_html = clean_html_for_regex(page_html)
    for pattern in STRICT_HTML_REGEXES:
        m = re.search(pattern, raw_html, flags=re.I | re.S)
        if m:
            return normalize_coupon_value(m.group(1)), "content", m.group(0)

    full_text = normalize_text(soup.get_text(" ", strip=True))
    coupon, snippet = extract_coupon_from_text(full_text)
    if coupon is not None:
        return coupon, "content", snippet if snippet else ""

    return None, None, ""


def try_extract_coupon_with_fallback(driver, queries):
    if not queries:
        return None, "", "查無搜尋關鍵字", ""

    query = queries[0]
    page_html = google_search(driver, query)
    coupon, found_by, debug_snippet = extract_coupon_from_html(page_html, query=query)

    if coupon is not None:
        if found_by == "label":
            return coupon, query, "成功(直接第二輪-欄位名稱)", debug_snippet
        elif found_by == "content":
            return coupon, query, "成功(直接第二輪-頁面內容)", debug_snippet
        else:
            return coupon, query, "成功(直接第二輪)", debug_snippet

    return None, query, "直接第二輪未找到", ""


# =========================================
# Step 1：左邊原本程式碼邏輯
# =========================================
def process_step1():
    df = pd.read_excel(INPUT_FILE, sheet_name=INPUT_SHEET_NAME)
    print("✅ [Step 1] 已讀取原始資料")

    bond_results = []
    other_results = []

    for _, row in df.iterrows():
        ename = str(row.get('BND_ENAME', '')).strip()
        date_match = re.search(r'(\d{2}/\d{2}/\d{2})', ename)
        is_perp = "PERP" in ename.upper()

        if date_match or is_perp:
            parts = ename.split()

            try:
                if date_match:
                    end_str = date_match.group(1)
                    sort_val = datetime.strptime(end_str, '%m/%d/%y')
                else:
                    end_str = "PERP"
                    sort_val = datetime(2099, 12, 31)

                end_idx = None
                for i, p in enumerate(parts):
                    if date_match and p == end_str:
                        end_idx = i
                        break
                    elif is_perp and p.upper() == "PERP":
                        end_idx = i
                        break

                if end_idx is None:
                    raise ValueError("找不到日期或 PERP 位置")

                raw_coupon = " ".join(parts[1:end_idx]).strip()
                coupon_val = convert_coupon(raw_coupon)

                res = row.to_dict()
                res['coupon rate'] = coupon_val
                res['_sort_date'] = sort_val

                if coupon_val is not None:
                    bond_results.append(res)
                else:
                    other_results.append(res)

            except Exception:
                res = row.to_dict()
                res['coupon rate'] = None
                res['_sort_date'] = datetime.max
                other_results.append(res)
        else:
            res = row.to_dict()
            res['coupon rate'] = None
            res['_sort_date'] = datetime.max
            other_results.append(res)

    bond_df = pd.DataFrame(bond_results)
    if not bond_df.empty:
        bond_df = bond_df.sort_values(by=['_sort_date', 'coupon rate'], ascending=[True, True])

    other_df = pd.DataFrame(other_results)
    final_df = pd.concat([bond_df, other_df], ignore_index=True)

    ref_df = pd.read_excel(REF_FILE)
    print("✅ [Step 1] 已讀取對照檔 Bond.xlsx")

    required_cols = ['IDFP_ISIN', 'REF_INSTR_INSTRUMENT_NAME']
    for col in required_cols:
        if col not in ref_df.columns:
            raise KeyError(f"Bond.xlsx 缺少欄位: {col}")

    ref_df['temp_rate'] = ref_df['REF_INSTR_INSTRUMENT_NAME'].apply(extract_rate_from_text)

    lookup_dict = (
        ref_df[['IDFP_ISIN', 'temp_rate']]
        .dropna(subset=['IDFP_ISIN'])
        .drop_duplicates(subset=['IDFP_ISIN'], keep='first')
        .set_index('IDFP_ISIN')['temp_rate']
        .to_dict()
    )

    if 'ISIN_CODE' not in final_df.columns:
        raise KeyError(f"結果檔缺少欄位: ISIN_CODE，目前欄位為 {list(final_df.columns)}")

    fill_count = 0

    def fill_empty_rate(row):
        nonlocal fill_count
        current_rate = row.get('coupon rate')

        if is_empty_coupon(current_rate):
            isin = str(row.get('ISIN_CODE', '')).strip()
            if isin:
                matched_rate = lookup_dict.get(isin)
                if matched_rate is not None and not pd.isna(matched_rate):
                    fill_count += 1
                    return matched_rate

        return current_rate

    final_df['coupon rate'] = final_df.apply(fill_empty_rate, axis=1)
    print(f"✅ [Step 1] 已完成 ISIN 對照與利率補全，共補 {fill_count} 筆")

    before_count = len(final_df)

    final_df = final_df[
        ~(
            final_df['ISIN_CODE'].astype(str).str.strip().str.upper().str.startswith('XS') &
            final_df['coupon rate'].apply(is_empty_coupon)
        )
    ].copy()

    removed_count = before_count - len(final_df)
    print(f"✅ [Step 1] 已剔除 ISIN_CODE 前綴為 XS 且找不到 coupon rate 的資料，共 {removed_count} 筆")

    final_df['_coupon_empty_flag'] = final_df['coupon rate'].apply(lambda x: 1 if is_empty_coupon(x) else 0)

    sort_cols = ['_coupon_empty_flag']
    ascending_list = [True]

    if '_sort_date' in final_df.columns:
        sort_cols.append('_sort_date')
        ascending_list.append(True)

    if 'coupon rate' in final_df.columns:
        sort_cols.append('coupon rate')
        ascending_list.append(True)

    final_df = final_df.sort_values(by=sort_cols, ascending=ascending_list, na_position='last').reset_index(drop=True)

    print("✅ [Step 1] 完成")
    return final_df


# =========================================
# Step 2：補 still missing 的 coupon rate
# =========================================
def process_step2(df: pd.DataFrame):
    if df.empty:
        print("⚠️ [Step 2] DataFrame 為空，略過")
        return df

    first_col_name = df.columns[0]
    coupon_col = find_coupon_rate_column(df)

    df[coupon_col] = df[coupon_col].where(~df[coupon_col].isna(), pd.NA)

    if "Google_Search_Query" not in df.columns:
        df["Google_Search_Query"] = ""
    if "Search_Status" not in df.columns:
        df["Search_Status"] = ""
    if "Debug_Snippet" not in df.columns:
        df["Debug_Snippet"] = ""

    driver = setup_driver()

    try:
        for idx, row in df.iterrows():
            try:
                current_coupon = row.get(coupon_col, "")

                if not is_missing_coupon(current_coupon):
                    df.at[idx, "Search_Status"] = "原本已有coupon rate"
                    continue

                queries = build_queries(row=row, first_col_name=first_col_name)
                coupon, used_query, status, debug_snippet = try_extract_coupon_with_fallback(driver, queries)

                df.at[idx, "Google_Search_Query"] = used_query
                df.at[idx, "Search_Status"] = status
                df.at[idx, "Debug_Snippet"] = debug_snippet

                if coupon is not None:
                    coupon_num = pd.to_numeric(coupon, errors="coerce")
                    if pd.notna(coupon_num):
                        df.at[idx, coupon_col] = float(coupon_num)

                print(f"[Step 2 - {idx + 1}] 查詢: {used_query} | 結果: {coupon} | 狀態: {status} | 片段: {debug_snippet}")

            except Exception as e:
                df.at[idx, "Search_Status"] = f"發生錯誤: {e}"
                print(f"[Step 2 - {idx + 1}] 發生錯誤，略過：{e}")

            time.sleep(ROW_SLEEP_SECONDS)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print("✅ [Step 2] 完成")
    return df


# =========================================
# 最後整理輸出
# =========================================
def finalize_and_export(df: pd.DataFrame):
    if 'ISIN_CODE' in df.columns:
        before_count = len(df)

        df = df[
            ~(
                df['ISIN_CODE'].astype(str).str.strip().str.upper().str.startswith('XS') &
                df['coupon rate'].apply(is_empty_coupon)
            )
        ].copy()

        removed_count = before_count - len(df)
        print(f"✅ [Final] 再次剔除 XS 且找不到 coupon rate 的資料，共 {removed_count} 筆")

    df['_coupon_empty_flag'] = df['coupon rate'].apply(lambda x: 1 if is_empty_coupon(x) else 0)

    sort_cols = ['_coupon_empty_flag']
    ascending_list = [True]

    if '_sort_date' in df.columns:
        sort_cols.append('_sort_date')
        ascending_list.append(True)

    if 'coupon rate' in df.columns:
        sort_cols.append('coupon rate')
        ascending_list.append(True)

    df = df.sort_values(by=sort_cols, ascending=ascending_list, na_position='last').reset_index(drop=True)

    # 刪除最後不要輸出的欄位
    drop_cols = [
        'Google_Search_Query',
        'Search_Status',
        'Debug_Snippet',
        '_coupon_empty_flag',
        '_sort_date'
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)

    print("\n---")
    print("✨ 全部任務完成！")
    print("📍 最後 Excel 已刪除 E、F、G 三欄")
    print("📍 已保留 coupon rate = 0 / 0.0")
    print("📍 已剔除 ISIN_CODE 前綴為 XS 且找不到 coupon rate 的資料")
    print(f"📂 輸出資料夾：{OUTPUT_DIR}")
    print(f"📂 結果已輸出至：{OUTPUT_FILE}")


# =========================================
# 主程式
# =========================================
def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"找不到原始檔案：{INPUT_FILE}")
    if not REF_FILE.exists():
        raise FileNotFoundError(f"找不到對照檔案：{REF_FILE}")

    print(f"📁 程式所在目錄：{BASE_DIR}")
    print(f"📁 output_data 資料夾：{OUTPUT_DIR}")

    df_step1 = process_step1()
    df_step2 = process_step2(df_step1)
    finalize_and_export(df_step2)


if __name__ == "__main__":
    main()
