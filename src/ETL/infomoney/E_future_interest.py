import requests
import datetime
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import logging
import re

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/E_future_interest.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

def get_nonce_with_selenium():
    url = 'https://www.infomoney.com.br/ferramentas/juros-futuros-di/'
    chromedriver_path = r"/home/lucas/.cache/selenium/chromedriver/linux64/chromedriver"

    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--headless")  

    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    driver.get(url)

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    nonce = None
    for script in soup.find_all('script'):
        if 'toolData' in script.text:
            match = re.search(r'"di_futuro_cotacoes_nonce":"(.*?)"', script.text)
            if match:
                nonce = match.group(1)
                break
    
    driver.quit()
    
    if not nonce:
        logger.error("Nonce não encontrado no toolData.")
    
    return nonce

def get_json_data(nonce):
    session = requests.Session()
    try:
        url = 'https://www.infomoney.com.br/wp-admin/admin-ajax.php'
        page_url = 'https://www.infomoney.com.br/ferramentas/juros-futuros-di/'

        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Referer': page_url,
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://www.infomoney.com.br',
            'Connection': 'keep-alive'
        }
        session.get(page_url, headers=headers)
        data = {
            'action': 'tool_contratos_di_futuro',
            'di_futuro_cotacoes_nonce': nonce
        }
        response = session.post(url, headers=headers, data=data)
        json_data = response.json()
        today = datetime.date.today()
        filename = f'data/raw/raw_interest_rates/juros_futuros_{today}.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
        print(f"Data retrieved succesfuly! File saved:{filename}")
        return json_data
    except requests.exceptions.RequestException as err:
        print("Error, check log for details")
        logger.error(err)

def run_future_interest_extractions():
    print("Running Future Interests extract")
    nonce = get_nonce_with_selenium()
    if nonce:
        get_json_data(nonce)
        print("Future Interests extract done!")
        print("_"*20)
    else:
        print("Failed to get nonce element")
