import pandas as pd 
import os 
import json

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

    # Expandir as colunas de dicionário
    df['vencimento'] = df['vencimento'].apply(lambda x: x['display'])
    
    file_date = os.path.basename(file).split('_')[-1].replace('.json', '')
    
    df['data_extracao'] = file_date


    # Drop as colunas originais de dicionário (opcional)
    df.drop(columns=['vencimento', 'data_atualizacao'], inplace=True)
    df.dropna(subset=['taxa'],inplace=True)

    # Exibir o DataFrame
    return df 

def run(input_folder,output_folder):
    files_list = os.listdir(input_folder)
    df_list = []
    for file in files_list:
        file_name = os.path.join(input_folder,file)
        df = parse_interest(file_name)
        df_list.append(df)
    
    final_df = pd.concat(df_list, ignore_index=False)
    final_df.to_csv(f'{output_folder}/interest_rate.csv',index=False)
        
    

if __name__=='__main__':
    input = 'data/raw/interest_rates'
    output = 'data/processed/interest_rates'
    run(input,output)