import pandas as pd 

def parse_m2(file_path, folder):
    df = pd.read_csv(file_path, sep=';', encoding='latin1')
    df.columns = ['mes', 'm2']
    df = df.iloc[:-1]
    df['data'] = pd.to_datetime(df['data'], format="%m/%Y").dt.strftime("%Y-%m-%d")
    df.to_csv(f"{folder}/base_m2.csv", index=False,sep=';')

def run_m2_transformations():
    file = 'data/raw/raw_monetary/m2_supply.csv'
    output = 'data/processed/monetary'
    parse_m2(file,output)