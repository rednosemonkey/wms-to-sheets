import os
from dotenv import load_dotenv
load_dotenv()
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.service_account import Credentials
import gspread
from selenium.webdriver.chrome.options import Options
import glob

# Create downloads directory if it doesn't exist
if not os.path.exists('downloads'):
   os.makedirs('downloads')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SHEET_ID = os.environ.get('SHEET_ID')
SHEET_NAME = 'WMS data'

credentials_info = {
   "private_key": os.environ.get('GOOGLE_PRIVATE_KEY'),
   "client_email": os.environ.get('GOOGLE_CLIENT_EMAIL'),
   "project_id": os.environ.get('GOOGLE_PROJECT_ID'),
   "token_uri": "https://oauth2.googleapis.com/token",
   "type": "service_account"
}

credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
client = gspread.authorize(credentials)

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    prefs = {
        "download.default_directory": os.path.join(os.getcwd(), "downloads"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=chrome_options)

def wms_download():
   driver = setup_driver()
   try:
       # Clean up existing files
       for f in glob.glob(os.path.join("downloads", "zaiko*.csv")):
           os.remove(f)

       driver.get("https://www.ec-zaiko.com/login.html")
       username_field = WebDriverWait(driver, 10).until(
           EC.presence_of_element_located((By.NAME, "disp_id"))
       )
       password_field = driver.find_element(By.NAME, "pass")
       username_field.send_keys(os.environ.get('WMS_USERNAME'))
       password_field.send_keys(os.environ.get('WMS_PASSWORD'))
       password_field.send_keys(Keys.RETURN)
       WebDriverWait(driver, 10).until(
           EC.presence_of_element_located((By.LINK_TEXT, "在庫管理"))
       ).click()
       search_button = WebDriverWait(driver, 10).until(
           EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="submit"][value="検索"]'))
       )
       search_button.click()
       csv_button = WebDriverWait(driver, 10).until(
           EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="submit"][value="全項目csv出力"]'))
       )
       csv_button.click()

       timeout = 30
       start_time = time.time()
       csv_files = []
       while time.time() - start_time < timeout:
           csv_files = glob.glob(os.path.join("downloads", "zaiko*.csv"))
           if csv_files:
               break
           time.sleep(1)
       if not csv_files:
           raise Exception("No zaiko CSV file found")
       csv_file = csv_files[0]
       process_csv_file(csv_file)
       os.remove(csv_file)
   except Exception as e:
    print(f"Error in wms_download: {e}")
   finally:
       driver.quit()

def process_csv_file(file_path):
   try:
       df = pd.read_csv(file_path, encoding='shift-jis').fillna('')
       columns_to_remove = ['ID', '商品規格２', 'バーコード']
       df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
       sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
       sheet.clear()
       if '実在庫数' in df.columns and '品番' in df.columns and '商品名' in df.columns:
           df['実在庫数'] = pd.to_numeric(df['実在庫数'], errors='coerce').fillna(0)
           df_filtered = df[df['実在庫数'] != 0]
           df_filtered = df_filtered[~df_filtered['品番'].str.contains('交換用スリーブ|Sticker', na=False)]
           df_filtered = df_filtered.sort_values(by=['商品名'], ascending=[True])
           df_filtered = df_filtered.astype(str)
           total_stock = pd.to_numeric(df_filtered['実在庫数'], errors='coerce').sum()
           total_row = pd.DataFrame({'品番': ['Total'], '実在庫数': [total_stock]})
           df_filtered = pd.concat([df_filtered, total_row], ignore_index=True)
       else:
           df_filtered = df
       df_filtered = df_filtered.fillna('')
       sheet.update([df_filtered.columns.values.tolist()] + df_filtered.values.tolist())
   except:
       pass

if __name__ == "__main__":
   wms_download()