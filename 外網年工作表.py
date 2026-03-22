# -*- coding: utf-8 -*-
import os, time, datetime, codecs, csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from openpyxl import Workbook

# === 1. 任務設定：只抓 2 份「年」報表 ========================================
TASKS = [
    {
        'name': '年上櫃證券成交統計表',
        'url': 'https://www.tpex.org.tw/zh-tw/mainboard/trading/info/statistics/year.html',
        'btn_selector': 'button.response[data-format="csv"]'
    },
    {
        'name': '年上櫃三大法人買賣金額統計表',
        'url': 'https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/summary/year.html',
        'btn_selector': 'button.response[data-format="csv"]'
    },
]

# === 2. 下載路徑（你的「年工作表」資料夾） ===============================
DOWNLOAD_ROOT = r'P:\經紀業務部\業務管理科\業務數據資料庫\AUTO\DownloadFile\外部\年工作表'
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

# === 3. Selenium 共用工具（與日工作表版相同） =============================
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
    driver.execute_cdp_cmd('Page.setDownloadBehavior',
                           {'behavior': 'allow', 'downloadPath': DOWNLOAD_ROOT})
    return driver

def clear_directory():
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

# === 4. 下載 + 轉成 XLSX =====================================================
def download_and_save_excel(task, y_tag):
    safe = task['name'].replace(' ', '_')
    prefix = f"{y_tag}_{safe}".lower()          # e.g. 2025_xxx.xlsx
    xlsx_path = os.path.join(DOWNLOAD_ROOT, f"{prefix}.xlsx")

    if os.path.exists(xlsx_path):
        os.remove(xlsx_path)                    # 同年度重跑直接覆蓋
        print(f'🔄 [{task["name"]}] 覆蓋舊檔')

    before = set(os.listdir(DOWNLOAD_ROOT))
    driver = setup_driver()
    try:
        driver.get(task['url'])
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, task['btn_selector']))
        ).click()
    except Exception as e:
        print(f'❌ [{task["name"]}] 下載失敗：{e}')
        driver.quit()
        return

    csv_name = wait_for_new_csv(before)
    raw_csv  = os.path.join(DOWNLOAD_ROOT, csv_name)
    final_csv = os.path.join(DOWNLOAD_ROOT, f"{prefix}.csv")
    os.rename(raw_csv, final_csv)

    wb, ws = Workbook(), Workbook().active
    ws.title = 'Data'
    with codecs.open(final_csv, 'r', encoding='cp950', errors='ignore') as f:
        for row in csv.reader(f):
            ws.append(row)
    wb.save(xlsx_path)
    os.remove(final_csv)
    driver.quit()
    print(f'✅ [{task["name"]}] 產出檔案：{xlsx_path}')

# === 5. 執行 ================================================================
if __name__ == '__main__':
    year_tag = datetime.date.today().strftime('%Y')   # 2025
    clear_directory()
    for t in TASKS:
        download_and_save_excel(t, year_tag)
    clear_directory()
