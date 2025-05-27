import pandas as pd 
import os 
import json
from utils.logger import get_logger
logger = get_logger(__name__)

def parse_interest(file):
    # Agora vamos montar o DataFrame
    with open(file, encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data, columns=[
        'ticker',
        'vencimento',
        'taxa',
        'variacao',
        'data_atualizacao',
        'volume',
        'taxa_ajustada',
        'comentario'
    ])
    df['vencimento'] = df['vencimento'].apply(lambda x: x['display'])
    file_date = os.path.basename(file).split('_')[-1].replace('.json', '')
    df['data_extracao'] = file_date
    df.drop(columns=['vencimento', 'data_atualizacao'], inplace=True)
    df.dropna(subset=['taxa'],inplace=True)
    return df 

