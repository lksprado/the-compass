import json 
import pandas as  pd 
from contracts import Railway, Toll
import pandera as pa 
from column_mapping.antt import traducao_mercadorias, tolls_columns

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def toll_parser(file:str) -> pd.DataFrame:
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    try:    
        df = pd.DataFrame(data['volume-trafego-praca'])
    except:
        df = pd.DataFrame(data['empresas_habilitadas_regular']) 
    df.columns = df.columns.str.lower()
    cols_to_rename = {old: new for old, new in tolls_columns.items() if old in df.columns}
    df = df.rename(columns=cols_to_rename)
    return df 

def make_toll_df(dataframe: pd.DataFrame):
    df = dataframe.copy()
    try:
        df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%Y-%m-%d').dt.date
    except:
        df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%d-%m-%Y').dt.date
    for col in ['concessionaria', 'sentido', 'praca', 'tipo_cobranca', 'categoria', 'tipo_de_veiculo']:
        if col in df.columns:
            df[col] = df[col].str.lower()
    df['volume_total'] = pd.to_numeric(df['volume_total'], errors='coerce')  # vira float
    df['volume_total'] = df['volume_total'].round().astype('Int64')  # arredonda e vira Int64 (nullable)
    print(df.dtypes)
    return df 

file = 'data/processed/errors/volume-trafego-praca-pedagio-2024.json'
df = toll_parser(file)
df = make_toll_df(df)
# print(df.loc[8400:8450])
try:
    Toll.validate(df, lazy=True)
except pa.errors.SchemaErrors as e:
    print("Erros de validação encontrados:")
    print(e.failure_cases)
except Exception as e:
    print("Outro erro:", repr(e))
