
import requests 
import json 
from datetime import date
import pandas as pd 
import os 
from utils.logger import get_logger

logger = get_logger(__name__)

def run_bitcoin_extraction():
    url = 'https://investidor10.com.br/api/criptomoedas/cotacoes/1/30/dollar'
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://investidor10.com.br/',
    'Connection': 'keep-alive',
    }
    try:
        response = requests.get(url, headers=headers)
        data = response.json() 
        extraction_date = date.today().strftime("%Y-%m-%d")
        with open(f'data/raw/raw_bitcoin/bitcoin_{extraction_date}.json','w') as f:
            json.dump(data,f,indent=4)
        logger.info(f"Data retrieved succesfuly! Bitcoin")
    except requests.exceptions.RequestException as err :
        logger.error(err)


def run_bitcoin_transformations():
    df_list =[]
    input_folder = 'data/raw/raw_bitcoin'
    output_folder = '/media/lucas/Files/2.Projetos/the-compass/data/processed/bitcoin'
    file_list = os.listdir(input_folder)
    
    try:
        for file in file_list:
            file_name = os.path.join(input_folder,file)
            data = pd.read_json(file_name, convert_dates=False)
            df_list.append(data)
        
        final_df = pd.concat(df_list,ignore_index=True)
        final_df["created_at"] = pd.to_datetime(final_df["created_at"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
        final_df = final_df.drop_duplicates()
        
        final_df.to_csv(f'{output_folder}/bitcoin.csv',sep=';',index=False)
        logger.info("Bitcoin dataframe converted to csv succesfuly")
        
    except Exception as e:
        logger.error(f"Something went wrong at conversion to csv with {output_folder}/bitcoin.csv --- {e}")
    
def run_bitcoin_etl():
    run_bitcoin_extraction()
    run_bitcoin_transformations()
    