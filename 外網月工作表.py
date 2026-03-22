# -*- coding: utf-8 -*-
import os
import time
import datetime
import codecs
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from openpyxl import Workbook

# === 1. 任務設定：只抓 3 份「月」報表 =======================================
BASE_URL = 'https://www.twse.com.tw'
TASKS = [
    # ① 月上櫃證券成交統計表（TPEX）
    {
        'name': '月上櫃證券成交統計表',
        'url': 'https://www.tpex.org.tw/zh-tw/mainboard/trading/info/statistics/month.html',
        'btn_selector': 'button.response[data-format="csv"]'
    },
    # ② 月三大法人買賣金額統計表（TWSE - 進站後要先切換月報表再查詢）
    {
        'name': '月三大法人買賣金額統計表',
        'url': f'{BASE_URL}/zh/trading/foreign/bfi82u.html',
        'pre_actions': [           # ⬅️ 先點「月報表」→再點「查詢」
            {'by': By.ID,  'selector': 'label2',                     'desc': '切換月報表'},
            {'by': By.CSS_SELECTOR, 'selector': 'div.submit > button.search', 'desc': '點擊查詢'}
        ],
        'btn_selector': 'button.csv'
    },
    # ③ 月上櫃三大法人買賣金額統計表（TPEX）
    {
        'name': '月上櫃三大法人買賣金額統計表',
        'url': 'https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/summary/month.html',
        'btn_selector': 'button.response[data-format="csv"]'
    },
]

# === 2. 儲存路徑（改成你的「月工作表」資料夾） =============================
DOWNLOAD_ROOT = r'P:\經紀業務部\業務管理科\業務數據資料庫\AUTO\DownloadFile\外部\月工作表'
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

# === 3. Selenium 共用工具 ====================================================
def setup_driver():
    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    opts.add_experimental_option('prefs', {
        'download.default_directory': DOWNLOAD_ROOT,
        'download.prompt_for_download': False,
        'profile.default_content_settings.popups': 0,
    })
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd('Page.setDownloadBehavior', {
        'behavior': 'allow', 'downloadPath': DOWNLOAD_ROOT
    })
    return driver

def clear_directory():
    """只留下 .xlsx；其餘暫存檔一律刪掉"""
    for f in os.listdir(DOWNLOAD_ROOT):
        if f.lower().endswith(('.tmp', '.crdownload', '.csv')):
            os.remove(os.path.join(DOWNLOAD_ROOT, f))

def wait_for_new_csv(before, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        diff = set(os.listdir(DOWNLOAD_ROOT)) - before
        for f in diff:
            if f.lower().endswith('.csv') and not f.lower().endswith('.crdownload'):
                return f
        time.sleep(0.3)
    raise RuntimeError('CSV 下載逾時')

# === 4. 下載 + 轉檔主流程 =====================================================
def download_and_save_excel(task, period_tag):
    safe = task['name'].replace(' ', '_')
    prefix = f"{period_tag}_{safe}".lower()
    xlsx_path = os.path.join(DOWNLOAD_ROOT, f"{prefix}.xlsx")

    if os.path.exists(xlsx_path):
        os.remove(xlsx_path)                      # 同月份重跑就覆蓋
        print(f'🔄 [{task["name"]}] 覆蓋舊檔')

    before = set(os.listdir(DOWNLOAD_ROOT))
    driver = setup_driver()
    try:
        driver.get(task['url'])

        # — 前置點擊（如果有設定）—
        for act in task.get('pre_actions', []):
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((act['by'], act['selector']))
            ).click()
            print(f'   · {act["desc"]}')

        # — 下載 —
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, task['btn_selector']))
        ).click()
    except Exception as e:
        print(f'❌ [{task["name"]}] 下載失敗：{e}')
        driver.quit()
        return
    # — 等待並搬到正式檔名 —
    csv_name = wait_for_new_csv(before)
    csv_path = os.path.join(DOWNLOAD_ROOT, csv_name)
    final_csv = os.path.join(DOWNLOAD_ROOT, f"{prefix}.csv")
    os.rename(csv_path, final_csv)

    # — CSV→XLSX (CP950 → UTF-8) —
    wb = Workbook()
    ws = wb.active
    ws.title = 'Data'
    with codecs.open(final_csv, 'r', encoding='cp950', errors='ignore') as fr:
        for row in csv.reader(fr):
            ws.append(row)
    wb.save(xlsx_path)
    os.remove(final_csv)          # 保持資料夾乾淨
    driver.quit()
    print(f'✅ [{task["name"]}] 產出檔案：{xlsx_path}')

# === 5. 執行 ================================================================
if __name__ == '__main__':
    clear_directory()
    period = datetime.date.today().strftime('%Y%m')   # 以「年月」當檔名前綴
    for task in TASKS:
        download_and_save_excel(task, period)
    clear_directory()
