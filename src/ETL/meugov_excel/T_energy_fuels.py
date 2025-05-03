import pandas as pd 

def fuel_parser(file_path, output_path):
    df = pd.read_excel(file_path, header=None)
    df.columns = df.iloc[16]
    df = df.iloc[17:].reset_index(drop=True)
    df.to_csv(f"{output_path}/fuels.csv", index=False, sep=';')
    return df 


def energy_parser(sheet,output_name, file_path, output_path):
    df = pd.read_excel(file_path, sheet_name=sheet)
    
    df.columns = [col.lower() for col in df.columns]
    df['data'] = pd.to_datetime(df['data'],format='%Y%m%d')
    df = df.drop(columns=['dataexcel','dataversao'])   
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.lower()
    
    df.to_csv(f"{output_path}/{output_name}.csv", index=False, sep=';')
    return df 

if __name__ == '__main__':
    # input = 'data/raw/fuel/precos_combustiveis.xlsx'
    # output = 'data/processed/energy'
    # fuel_parser(input,output)
    
    energy_input = 'data/raw/energy/Dados_abertos_Consumo_Mensal.xlsx'
    energy_output = 'data/processed/energy'
    
    energy_parser('SETOR INDUSTRIAL POR UF','consumo_setor_industrial_uf',energy_input,energy_output)