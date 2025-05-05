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
import re

def get_nonce_with_selenium():
    url = 'https://www.infomoney.com.br/ferramentas/juros-futuros-di/'
    chromedriver_path = r"/home/lucas/.cache/selenium/chromedriver/linux64/chromedriver"

    # Configurar opções do Chrome
    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--headless")  # Remova para depurar

    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    driver.get(url)

    # Obter o HTML renderizado
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # Procurar o nonce no toolData
    nonce = None
    for script in soup.find_all('script'):
        if 'toolData' in script.text:
            match = re.search(r'"di_futuro_cotacoes_nonce":"(.*?)"', script.text)
            if match:
                nonce = match.group(1)
                break
    
    driver.quit()
    
    if not nonce:
        print("Nonce não encontrado no toolData.")
    else:
        print(f"Nonce encontrado: {nonce}")
    
    return nonce

def get_json_data(nonce):
    session = requests.Session()
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

    # Visitar a página inicial para pegar cookies
    session.get(page_url, headers=headers)

    # Usar o nonce obtido
    data = {
        'action': 'tool_contratos_di_futuro',
        'di_futuro_cotacoes_nonce': nonce
    }

    # Fazer a requisição POST
    response = session.post(url, headers=headers, data=data)
    
    print(f"Status Code: {response.status_code}")
    print("Headers:", response.headers)
    
    try:
        json_data = response.json()
        today = datetime.date.today()
        filename = f'data/raw/interest_rates/juros_futuros_{today}.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
        print(f"JSON salvo em '{filename}'")
        return json_data
    except requests.exceptions.JSONDecodeError as e:
        print(f"Erro ao parsear JSON: {e}")
        return None

def run_interest():
    nonce = get_nonce_with_selenium()
    if nonce:
        json_data = get_json_data(nonce)
        if json_data:
            print("Dados obtidos com sucesso!")