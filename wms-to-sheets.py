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
    
    max_attempts = 3
    retry_delay = 10  # seconds

    for attempt in range(1, max_attempts + 1):
        try:
            return webdriver.Chrome(options=chrome_options)
        except Exception as e:
            if attempt == max_attempts:
                raise e
            print(f"Setup attempt {attempt} failed: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

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
        # Get current timestamp in Japan timezone
        from datetime import datetime
        from zoneinfo import ZoneInfo
        timestamp = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")
        
        df = pd.read_csv(file_path, encoding='shift-jis').fillna('')
        
        # Column mapping
        column_mapping = {
            '品番': 'Product No.',
            '商品名': 'Product Name',
            '商品規格１': 'No. of Units',
            'ロケーション1': 'Expiry Date',
            '実在庫数': 'Stock'
        }
        
        # Rename columns that exist in the DataFrame
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Clean up Expiry Date format
        if 'Expiry Date' in df.columns:
            df['Expiry Date'] = df['Expiry Date'].astype(str).apply(lambda x: x.replace('賞味期限', '') if '賞味期限' in x else x)
            
        # Extract units number from No. of Units column
        if 'No. of Units' in df.columns:
            def extract_units(text):
                text = str(text)
                if '入数' in text or '入り数' in text:
                    import re
                    numbers = re.findall(r'入[り]?数(\d+)', text)
                    return numbers[0] if numbers else text
                return text
            df['No. of Units'] = df['No. of Units'].apply(extract_units)
        
        columns_to_remove = ['ID', '商品規格２', 'バーコード', 'ロケーション2']
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
        sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        sheet.clear()
        
        # Add timestamp row first
        sheet.update(values=[[f'Last Updated: {timestamp}']], range_name='A1')
        
        if 'Stock' in df.columns and 'Product No.' in df.columns and 'Product Name' in df.columns:
            df['Stock'] = pd.to_numeric(df['Stock'], errors='coerce').fillna(0)
            df_filtered = df[df['Stock'] != 0]
            df_filtered = df_filtered[~df_filtered['Product No.'].str.contains('交換用スリーブ|Sticker', na=False)]
            df_filtered = df_filtered.sort_values(by=['Product Name'], ascending=[True])
            df_filtered = df_filtered.astype(str)
            total_stock = pd.to_numeric(df_filtered['Stock'], errors='coerce').sum()
            total_row = pd.DataFrame({'Product No.': ['Total'], 'Stock': [total_stock]})
            df_filtered = pd.concat([df_filtered, total_row], ignore_index=True)
        else:
            df_filtered = df
            
        df_filtered = df_filtered.fillna('')
        # Update data starting from row 2
        all_values = [df_filtered.columns.values.tolist()] + df_filtered.values.tolist()
        sheet.update(values=all_values, range_name='A2')
    except Exception as e:
        print(f"Error in process_csv_file: {e}")

if __name__ == "__main__":
   wms_download()