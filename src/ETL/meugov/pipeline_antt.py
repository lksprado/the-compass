from src.ETL.meugov.E_antt import MeuGov
from src.ETL.meugov.T_antt import *
from src.ETL.meugov.E_energy_fuels import *
from src.ETL.meugov.T_energy_fuels import *
import json
from datetime import datetime
import re 

# OBTEM OS METADOS DA API PARA OBTER LINK MAIS ATUALIZADO
def run_railway_api_metadata(api_url,output_path):
    file_links = MeuGov(api_url)
    file_links.get_json(output_path)

def get_latest_update(api_metadata_json_file):
    with open(api_metadata_json_file,'r',encoding='utf-8') as f:
        dados = json.load(f)
    data_referencia = dados["dataUltimaAtualizacaoArquivo"].split(" ")[0]

    recursos_filtrados = [
        recurso for recurso in dados.get("recursos", [])
        if recurso.get("dataCatalogacao") and 
        recurso["dataCatalogacao"].split(" ")[0] == data_referencia
    ]

    for recurso in recursos_filtrados:
        print(recurso["titulo"], recurso["link"])
    return



def run_railway_extraction():
    print("Running ANTT extracts")
    railway_url = 'https://dados.antt.gov.br/dataset/438a5184-09db-49a3-88c8-0bad418b4409/resource/fecf6b19-6e91-42d1-baf0-ee64b8a5d246/download/producao_origem_destino_2025.json'
    railways = MeuGov(railway_url)
    railways.get_json('data/raw/raw_meugov/antt_ferrovias/')

def run_tolls_extraction():
    tolls_url = 'https://dados.antt.gov.br/dataset/5bf70ec3-b24e-4f73-99a0-78b200f5e915/resource/8a216ae6-0173-4752-a946-8fae35f9cde7/download/volume-trafego-praca-pedagio-2025.json'
    tolls = MeuGov(tolls_url)
    tolls.get_json('data/raw/raw_meugov/antt_pedagio/')

def concatenate_processed_railway_files_to_single_csv():
    processed_path = 'data/processed/antt_ferrovias/arquivos'
    PROCESSED_METADATA_PATH = '/media/lucas/Files/2.Projetos/the-compass/data/processed/antt_ferrovias/railway_table.csv'

    files = [f for f in os.listdir(processed_path) if f.endswith('.csv')]
    all_dfs = []

    for file in files:
        file_path = os.path.join(processed_path, file)
        try:
            df = pd.read_csv(file_path, sep=";")
            all_dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler {file_path}: {e}")

    if all_dfs:
        df_final = pd.concat(all_dfs, ignore_index=True)
        df_final.to_csv(PROCESSED_METADATA_PATH, sep=";", index=False)
        print(f"Concatenação concluída: {PROCESSED_METADATA_PATH}")
    else:
        print("Nenhum arquivo CSV válido encontrado para concatenação.")

def update_railway_consolidated_csv():
    processed_path = 'data/processed/antt_ferrovias/arquivos'
    consolidated_path = 'data/processed/antt_ferrovias/railway_table.csv'

    # Carregar o CSV consolidado atual, se existir
    if os.path.exists(consolidated_path):
        df_consolidated = pd.read_csv(consolidated_path, sep=";")
        print(f"Arquivo consolidado carregado: {consolidated_path}")
    else:
        df_consolidated = pd.DataFrame()
        print("Nenhum arquivo consolidado encontrado. Criando novo.")

    # Listar os CSVs processados
    files = [f for f in os.listdir(processed_path) if f.endswith('.csv')]

    for file in files:
        file_path = os.path.join(processed_path, file)

        try:
            df_new = pd.read_csv(file_path, sep=";")

            if "mes_ano" not in df_new.columns:
                print(f"Aviso: 'mes_ano' não encontrado em {file}. Pulando arquivo.")
                continue

            mes_ano_novos = df_new['mes_ano'].unique()

            print(f"Processando {file} | mes_ano detectado: {mes_ano_novos}")

            # Remover do consolidado os registros que tenham o mesmo mes_ano
            if not df_consolidated.empty:
                df_consolidated = df_consolidated[~df_consolidated['mes_ano'].isin(mes_ano_novos)]

            # Adicionar os dados novos
            df_consolidated = pd.concat([df_consolidated, df_new], ignore_index=True)

        except Exception as e:
            print(f"Erro ao processar {file_path}: {e}")

    # Salvar CSV consolidado atualizado
    df_consolidated.to_csv(consolidated_path, sep=";", index=False)
    print(f"Arquivo consolidado atualizado: {consolidated_path}")


def run_railway_transformation_to_csv():
    input_path = 'data/raw/raw_meugov/antt_ferrovias'
    output_path = 'data/processed/antt_ferrovias/arquivos'
    error_path = 'data/processed/errors'

    files = [f for f in os.listdir(input_path) if f.endswith('.json')]

    # Regex para capturar anos nos nomes dos arquivos
    year_pattern = re.compile(r'(20\d{2})')

    # Mapeia arquivos para seus respectivos anos
    file_year_map = {}
    for file in files:
        match = year_pattern.search(file)
        if match:
            year = int(match.group(1))
            file_year_map[file] = year

    if not file_year_map:
        logger.warning("Nenhum arquivo com ano identificado foi encontrado.")
        return

    # Descobre o maior ano
    most_recent_year = max(file_year_map.values())
    logger.info(f"Processando apenas arquivos do ano mais recente: {most_recent_year}")

    # Filtra os arquivos para processar apenas os do ano mais recente
    recent_files = [f for f, y in file_year_map.items() if y == most_recent_year]

    for file in recent_files:
        final_name = os.path.splitext(os.path.basename(file))[0]
        file_path = os.path.join(input_path, file)

        try:
            df = make_railway_df(file_path)
            df.columns = df.columns.str.lower()
            Railway.validate(df, lazy=True)
            df.to_csv(f"{output_path}/{final_name}.csv", sep=";", index=False)
        except pa.errors.SchemaErrors as exc:
            invalid_indices = exc.failure_cases["index"].unique()
            df_valid = df.drop(index=invalid_indices)

            if not df_valid.empty and len(invalid_indices) > 0:
                logger.warning(f"{file}: {len(invalid_indices)} linhas inválidas removidas")
                df_valid.to_csv(f"{output_path}/{final_name}.csv", sep=";", index=False)
            else:
                logger.warning(f"{file}: Todas as linhas inválidas. Arquivo movido para {error_path}.")
                shutil.copyfile(file_path, os.path.join(error_path, file))





def run_railway_transformation():
    input_path = 'data/raw/raw_meugov/antt_ferrovias'
    output_path = 'data/processed/antt_ferrovias'  
    error_path = 'data/processed/errors'
    PROCESSED_METADATA_PATH = 'src/ETL/meugov/antt_ferrovias_processadas.csv'
    
    if os.path.exists(PROCESSED_METADATA_PATH):
        processed_df = pd.read_csv(PROCESSED_METADATA_PATH)
        processed_files = set(processed_df['filename'])
    else:
        processed_files = set()

    files = os.listdir(input_path)
    ls = [] 
    processed =[]
    
    for file in files:
        if file.endswith('.json') and file not in processed_files:
            file_name = os.path.join(input_path, file)

            try:
                df = make_railway_df(file_name)
                df.columns = df.columns.str.lower()
                Railway.validate(df, lazy=True)
                ls.append(df)
                processed.append(file)
            except pa.errors.SchemaErrors as exc:
                # Coletar índices das linhas inválidas
                invalid_indices = exc.failure_cases["index"].unique()
                # Remover linhas inválidas
                df_valid = df.drop(index=invalid_indices)
                if not df_valid.empty and len(invalid_indices) > 0:
                    logger.warning(f"{file}: {len(invalid_indices)} removed invalid rows")
                    ls.append(df_valid)
                else:
                    logger.warning(f"{file}: All rows compromised. Moving file to error folder.")
                    shutil.copyfile(file_name, os.path.join(error_path, file))
    if processed:
        pd.DataFrame({'filename': processed}).to_csv(
            PROCESSED_METADATA_PATH, mode='a', header=not os.path.exists(PROCESSED_METADATA_PATH), index=False
        )
    if ls:
        final_df = pd.concat(ls, ignore_index=True)
        final_df.to_csv(f"{output_path}/railway_table.csv", sep=";", index=False)
    else:
        logger.warning("Dataframe creation has failed. No valid rows at all.")

def run_toll_transformation():
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
            df = make_toll_df(df)
            Toll.validate(df, lazy=True)
            ls.append(df)

        except KeyError as ke:
            print(f"{file}: JSON key error: ({ke}). Moving file to error folder.")
            shutil.copyfile(file_name, os.path.join(error_path, file))
            continue

        except pa.errors.SchemaErrors as exc:
            invalid_indices = exc.failure_cases["index"].unique()
            df_valid = df.drop(index=invalid_indices)

            if not df_valid.empty and len(invalid_indices)>0:
                logger.warning(f"{file}: {len(invalid_indices)} removed invalid rows")
                ls.append(df_valid)
            else:
                logger.warning(f"{file}: All rows compromised. Moving file to error folder.")
                shutil.copyfile(file_name, os.path.join(error_path, file))
            continue

        except Exception as e:
            logger.error(f"{file}: Something went wrong: {e}")
            shutil.copyfile(file_name, os.path.join(error_path, file))
            continue
    
    df = df.drop_duplicates()
    # Concatena tudo e salva
    if ls:
        final_df = pd.concat(ls, ignore_index=True)
        final_df.to_csv(f"{output_path}/toll_table.csv", sep=";", index=False)
    else:
        logger.warning("Dataframe creation has failed. No valid rows at all.")

def run_railway_pipeline():
    run_railway_extraction()
    run_railway_transformation()

def run_toll_pipeline():
    run_tolls_extraction()
    run_toll_transformation()

if __name__=='__main__':
    # run_railway_pipeline()
    # run_railway_api_metadata()
    # get_latest_update('data/raw/raw_meugov/antt_ferrovias/api_metadata/api_metadata438a5184-09db-49a3-88c8-0bad418b4409')
    # run_railway_transformation_to_csv()
    concatenate_processed_railway_files_to_single_csv()