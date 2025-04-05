import json 
import pandas as pd
import os 
import shutil
import pandera as pa
from contracts import Railway

def railway_parser(file:str) -> pd.DataFrame:
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data['producao_origem_destino'])
    df.columns = df.columns.str.lower()
    return df 
    
def make_railway_df(dataframe: pd.DataFrame):
    df = dataframe.copy()
    df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%m/%Y').dt.date
    df['tu'] = df['tu'].str.replace('.', '', regex=False).astype(int)
    df['tku'] = df['tku'].str.replace('.', '', regex=False).astype(int)
    df['estimated_distance_km'] = df.apply(lambda row: row['tku'] / row['tu'] if row['tu'] != 0 else None, axis=1)
    df['ferrovia'] = df['ferrovia'].str.lower()
    df['mercadoria_antt'] = df['mercadoria_antt'].str.lower()
    df['estacao_origem'] = df['estacao_origem'].str.lower()
    df['uf_origem'] = df['uf_origem'].str.upper()
    df['estacao_destino'] = df['estacao_destino'].str.upper()
    df['uf_destino'] = df['estacao_destino']
    df = df.drop(columns=['estacao_destino'], errors='ignore')
    return df 

def run_railway():
    input_path = 'data/raw/antt_ferrovias'
    output_path = 'data/processed/antt_ferrovias'  
    error_path = 'data/processed/errors'
    
    files = os.listdir(input_path)
    ls = [] 
    
    for file in files:
        if file.endswith('.json'):
            file_name = os.path.join(input_path, file)

            try:
                df = railway_parser(file_name)
                df = make_railway_df(df)
                # Tenta validar com lazy=True para capturar todas as falhas
                Railway.validate(df, lazy=True)
                ls.append(df)
                print(f"{file}: Nenhuma linha inválida")

            except pa.errors.SchemaErrors as exc:
                # Coletar índices das linhas inválidas
                invalid_indices = exc.failure_cases["index"].unique()
                
                # Remover linhas inválidas
                df_valid = df.drop(index=invalid_indices)
                
                if not df_valid.empty:
                    print(f"{file}: {len(invalid_indices)} linhas inválidas removidas")
                    ls.append(df_valid)
                else:
                    print(f"{file}: Todas as linhas inválidas. Movendo arquivo para pasta de erro.")
                    shutil.copyfile(file_name, os.path.join(error_path, file))

    # Concatena tudo e salva
    if ls:
        final_df = pd.concat(ls, ignore_index=True)
        final_df.to_csv(f"{output_path}/railway_table.csv", sep=";", index=False)
        print("Done!")
    else:
        print("Nenhum dado válido encontrado.")
                
                
if __name__ == '__main__':
    run_railway()
