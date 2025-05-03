import json 
import pandas as pd
import os 
import shutil
import pandera as pa
from contracts import Railway, Toll
from column_mapping.antt import traducao_mercadorias, tolls_columns

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
    df['estimated_distance_km'] = df['estimated_distance_km'].fillna(0).astype(int)
    df['ferrovia'] = df['ferrovia'].str.lower()
    df['mercadoria_antt'] = df['mercadoria_antt'].str.lower()
    df['mercadoria_en'] = df['mercadoria_antt'].apply(lambda x: traducao_mercadorias.get(x, x))
    df['estacao_origem'] = df['estacao_origem'].str.lower()
    df['uf_origem'] = df['uf_origem'].str.upper()
    df['estacao_destino'] = df['estacao_destino'].str.upper()
    df['uf_destino'] = df['estacao_destino']
    df = df.drop(columns=['estacao_destino'], errors='ignore')
    return df 

def run_railway():
    input_path = 'data/raw/raw_meugov/antt_ferrovias'
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
    print(f"{file} --- Json Processed")
    return df 

def make_toll_df(dataframe: pd.DataFrame):
    df = dataframe.copy()
    try:
        df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%Y-%m-%d').dt.date
    except:
        df['mes_ano'] = pd.to_datetime(df['mes_ano'], format='%d-%m-%Y').dt.date
    for col in ['concessionaria', 'praca', 'tipo_cobranca', 'tipo_de_veiculo']:
            df[col] = df[col].str.lower()
    df['volume_total'] = pd.to_numeric(df['volume_total'], errors='coerce')
    df['volume_total'] = df['volume_total'].round().astype('Int64') 
    df = df[['concessionaria', 'mes_ano','praca','tipo_cobranca','tipo_de_veiculo','volume_total']]
    print("DF OK")
    return df 

def run_toll():
    input_path = 'data/raw/raw_meugov/antt_pedagio'
    output_path = 'data/processed/antt_pedagio'  
    error_path = 'data/processed/errors'
    
    files = os.listdir(input_path)
    ls = [] 
    
    for file in files:
        if not file.endswith('.json'):
            continue
        
        file_name = os.path.join(input_path, file)

        try:
            df = toll_parser(file_name)
            df = make_toll_df(df)
            Toll.validate(df, lazy=True)
            ls.append(df)
            print(f"{file}: Nenhuma linha inválida")

        except KeyError as ke:
            print(f"{file}: Chave ausente no JSON ({ke}). Movendo para pasta de erro.")
            shutil.copyfile(file_name, os.path.join(error_path, file))
            continue

        except pa.errors.SchemaErrors as exc:
            invalid_indices = exc.failure_cases["index"].unique()
            df_valid = df.drop(index=invalid_indices)

            if not df_valid.empty:
                print(f"{file}: {len(invalid_indices)} linhas inválidas removidas")
                ls.append(df_valid)
            else:
                print(f"{file}: Todas as linhas inválidas. Movendo para pasta de erro.")
                shutil.copyfile(file_name, os.path.join(error_path, file))
            continue

        except Exception as e:
            print(f"{file}: Erro inesperado: {e}")
            shutil.copyfile(file_name, os.path.join(error_path, file))
            continue
    
    df = df.drop_duplicates()
    # Concatena tudo e salva
    if ls:
        final_df = pd.concat(ls, ignore_index=True)
        final_df.to_csv(f"{output_path}/toll_table.csv", sep=";", index=False)
        print("Done!")
    else:
        print("Nenhum dado válido encontrado.")


if __name__ == '__main__':
    # run_railway()
    run_toll()
