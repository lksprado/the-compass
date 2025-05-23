import json 
import pandas as pd
import os
import shutil
import pandera as pa
from src.contracts import Railway, Toll
from src.column_mapping.antt import traducao_mercadorias, tolls_columns
from utils.logger import get_logger
logger = get_logger(__name__)

def make_railway_df(file:str):
    filename = os.path.basename(file)
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data['producao_origem_destino'])
    df.columns = df.columns.str.lower()
    df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%m/%Y').dt.date
    df['tu'] = df['tu'].str.replace('.', '', regex=False).astype(int)
    df['tku'] = df['tku'].str.replace('.', '', regex=False).astype(int)
    df['estimated_distance_km'] = df.apply(lambda row: row['tku'] / row['tu'] if row['tu'] != 0 else None, axis=1)
    df['estimated_distance_km'] = df['estimated_distance_km'].fillna(0).astype(int)
    df['ferrovia'] = df['ferrovia'].str.lower()
    df['mercadoria_antt'] = df['mercadoria_antt'].str.lower()
    df['mercadoria_en'] = df['mercadoria_antt'].apply(lambda x: traducao_mercadorias.get(x, x))
    df['estacao_origem'] = df['estacao_origem'].str.lower()
    df['uf_origem'] = df['uf_origem'].str.upper()
    df['estacao_destino'] = df['estacao_destino'].str.upper()
    df['uf_destino'] = df['estacao_destino']
    df = df.drop(columns=['estacao_destino'], errors='ignore')
    df['file'] = filename
    return df 

def make_toll_df(file:str):
    filename = os.path.basename(file)
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    try:    
        df = pd.DataFrame(data['volume-trafego-praca'])
    except:
        df = pd.DataFrame(data['empresas_habilitadas_regular']) 
    df.columns = df.columns.str.lower()
    cols_to_rename = {old: new for old, new in tolls_columns.items() if old in df.columns}
    df = df.rename(columns=cols_to_rename)
    try:
        df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%Y-%m-%d').dt.date
    except:
        df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%d-%m-%Y').dt.date
    for col in ['concessionaria', 'praca', 'tipo_cobranca', 'tipo_de_veiculo']:
            df[col] = df[col].str.lower()
    df['volume_total'] = pd.to_numeric(df['volume_total'], errors='coerce')
    df['volume_total'] = df['volume_total'].round().astype('Int64') 
    df = df[['concessionaria', 'mes_ano','praca','tipo_cobranca','tipo_de_veiculo','volume_total']]
    df['file'] = filename
    return df 


if __name__ =='__main__':
    data = make_railway_df('data/raw/raw_meugov/antt_ferrovias/producao_origem_destino_2006.json')
    print(data.head())
