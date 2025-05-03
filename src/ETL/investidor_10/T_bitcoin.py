import pandas as pd 
import os 
def parse_bitcoin(json_file: str)-> pd.DataFrame:
    data = pd.read_json(json_file)
    return data 

def run_bitcoin(input_folder, output_folder):
    df_list =[]
    file_list = os.listdir(input_folder)
    
    for file in file_list:
        file_name = os.path.join(input_folder,file)
        data = pd.read_json(file_name)
        df_list.append(data)
    
    final_df = pd.concat(df_list,ignore_index=True)
    final_df = final_df.drop_duplicates()
    
    final_df.to_csv(f'{output_folder}/bitcoin.csv',index=False)
    

if __name__ == '__main__':
    input = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_bitcoin'
    output = '/media/lucas/Files/2.Projetos/the-compass/data/processed/bitcoin'
    
    run_bitcoin(input,output)
    