import requests
import datetime
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
from utils.logger import get_logger

logger = get_logger(__name__)

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
        logger.info(f"Data retrieved succesfuly! File saved:{filename}")
        logger.info("-"*50)
        return json_data
    except requests.exceptions.RequestException as err:
        logger.error(err)


