from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
import fitz  # PyMuPDF
import io
import re

# 🧠 資料抽取函式
# 🧠 資料抽取函式
def extract_fields(pdf_text, is_quarterly=False):
    patterns = {
        "成交量週轉率": r"成交量週轉率：([\d\.]+)%",
        "當沖成交值占比": r"當沖成交值占比：([\d\.]+)%",
        "平均每日當沖戶數": r"平均每日當沖戶數：([\d,]+)戶",
        "ETF成交值占比": r"ETF成交值占比：([\d\.]+)%",
        "自然人成交值比重": r"自然人成交值比重：([\d\.]+)%",
        "外資成交值比重": r"外資成交值比重：([\d\.]+)%",
        "有交易戶數": r"有交易戶數：([\d,]+)戶",
        "定期定額投資金額": r"定期定額投資金額：([\d,]+)元"
    }

    if is_quarterly:
        patterns.update({
            "成交值5億元以上之自然人": r"成交值5億元以上之自然人(?:人數)?[^。\d]*([\d,]+)人",
            "成交值1億元至5億元之自然人": r"成交值1億元至5億元之自然人(?:人數)?[^。\d]*([\d,]+)人",
            "成交值1億元以下之自然人": r"成交值1億元以下之自然人(?:人數)?[^。\d]*([\d,]+)人",
            "自然人占集中市場總成交值比重": r"自然人占集中市場總成交值比重(?:為)?[^。\d]*([\d\.]+)%",
            "外資": r"外資(?:則)?(?:為)?[^。\d]*([\d\.]+)%",
            "國內法人": r"國內法人(?:則)?(?:為)?[^。\d]*([\d\.]+)%"
        })

    # 📌 明確對應欄位單位（避免誤判）
    field_units = {
        "成交量週轉率": "%",
        "當沖成交值占比": "%",
        "平均每日當沖戶數": "戶",
        "ETF成交值占比": "%",
        "自然人成交值比重": "%",
        "外資成交值比重": "%",
        "有交易戶數": "戶",
        "定期定額投資金額": "元",
        "成交值5億元以上之自然人": "人",
        "成交值1億元至5億元之自然人": "人",
        "成交值1億元以下之自然人": "人",
        "自然人占集中市場總成交值比重": "%",
        "外資": "%",
        "國內法人": "%"
    }

    result = {}
    for field, pattern in patterns.items():
        match = re.search(pattern, pdf_text)
        if match:
            num = match.group(1).replace(',', '')
            unit = field_units.get(field, '')
            result[field] = f"{num}{unit}" if unit else num
        else:
            result[field] = None
    return result

# 設定 Chrome 選項
chrome_options = Options()
chrome_options.add_argument("--start-maximized")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get("https://wwwc.twse.com.tw/zh/search.html")
time.sleep(2)

# 勾選「投資人知識網」
driver.find_element(By.ID, "site5").click()
time.sleep(0.5)

# 輸入搜尋關鍵字
search_input = driver.find_element(By.NAME, "q")
search_input.clear()
search_input.send_keys("集中市場相關數據")
search_input.send_keys(Keys.ENTER)
time.sleep(5)

# 最多點擊「顯示更多搜尋結果」按鈕
click_count = 0
max_clicks = 3

while click_count < max_clicks:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    more_buttons = driver.find_elements(By.CLASS_NAME, "more")
    if more_buttons:
        try:
            more_buttons[-1].click()
            click_count += 1
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        except Exception as e:
            print(f"點擊失敗：{e}")
            break
    else:
        print("沒有『顯示更多搜尋結果』按鈕了")
        break

# 📥 擷取目標年份
target_keywords = ["2024年", "2025年"]
results_by_month = {}
structured_data = {}

# 處理符合的 PDF
for keyword in target_keywords:
    print(f"\n=== 正在處理：{keyword} ===")
    pdf_links = driver.find_elements(By.PARTIAL_LINK_TEXT, keyword)
    seen_links = set()

    for link in pdf_links:
        href = link.get_attribute("href")
        text = link.text
        if href and href.endswith(".pdf") and href not in seen_links:
            seen_links.add(href)
            print(f"找到 PDF：{href}")
            try:
                match = re.search(r"(20\d{2})年(\d{1,2})月", text)
                if match:
                    year, month = match.group(1), match.group(2).zfill(2)
                    ym_key = f"{year}-{month}"
                else:
                    year_match = re.search(r"(20\d{2})年", text)
                    ym_key = f"{year_match.group(1)}-00" if year_match else "未知"

                response = requests.get(href)
                if response.status_code == 200:
                    pdf_stream = io.BytesIO(response.content)
                    doc = fitz.open("pdf", pdf_stream)
                    content = "\n".join([page.get_text() for page in doc])
                    doc.close()

                    results_by_month[ym_key] = content
                    is_quarterly = "季" in content
                    structured_data[ym_key] = extract_fields(content, is_quarterly)
                else:
                    print("PDF 無法下載，狀態碼：", response.status_code)
            except Exception as e:
                print(f"處理失敗：{e}")

# ✅ 輸出結果
for ym in sorted(structured_data.keys()):
    print(f"\n📊 【{ym}】")
    for field, value in structured_data[ym].items():
        print(f"{field}: {value}")

# 結束 Selenium
driver.quit()
