import requests 
import json 
from datetime import date
import logging

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/E_bitcoin.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

def get_json():
    url = 'https://investidor10.com.br/api/criptomoedas/cotacoes/1/30/dollar'
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://investidor10.com.br/',
    'Connection': 'keep-alive',
    }
    try:
        print(f"Requesting {url}...")
        response = requests.get(url, headers=headers)
        data = response.json() 
        extraction_date = date.today().strftime("%Y-%m-%d")
        with open(f'data/raw/raw_bitcoin/bitcoin_{extraction_date}.json','w') as f:
            json.dump(data,f,indent=4)
    except requests.exceptions.RequestException as err :
        print("Error, check log for details")
        logger.error(err)

def run_bitcoin_extracts():
    print("Running Bitcoin extract")
    get_json()
    print("Bitcoin extract done!")
    print("_"*20)