import pandas as pd 
import os 


def parse_bitcoin(input_folder, output_folder):
    df_list =[]
    file_list = os.listdir(input_folder)
    
    for file in file_list:
        file_name = os.path.join(input_folder,file)
        data = pd.read_json(file_name, convert_dates=False)
        df_list.append(data)
    
    final_df = pd.concat(df_list,ignore_index=True)
    final_df["created_at"] = pd.to_datetime(final_df["created_at"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
    final_df = final_df.drop_duplicates()
    
    final_df.to_csv(f'{output_folder}/bitcoin.csv',sep=';',index=False)
    

def run_bitcoin_transformations():
    input = 'data/raw/raw_bitcoin'
    output = '/media/lucas/Files/2.Projetos/the-compass/data/processed/bitcoin'
    
    parse_bitcoin(input,output)

run_bitcoin_transformations()