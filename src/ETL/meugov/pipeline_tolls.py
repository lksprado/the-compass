from src.ETL.meugov.E_antt import MeuGov
from src.ETL.meugov.T_antt import *
import json
from utils.logger import get_logger
logger = get_logger(__name__)

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

INPUT_PATH = 'data/raw/raw_meugov/antt_pedagio/'
OUTPUT_PATH = 'data/processed/antt_pedagio/arquivos'  
ERROR_PATH = 'data/processed/errors'
CONSOLIDATED_PATH = 'data/processed/antt_pedagio/toll_table.csv'

def run_tolls_extraction():
    tolls_url = 'https://dados.antt.gov.br/dataset/5bf70ec3-b24e-4f73-99a0-78b200f5e915/resource/8a216ae6-0173-4752-a946-8fae35f9cde7/download/volume-trafego-praca-pedagio-2025.json'
    tolls = MeuGov(tolls_url)
    tolls.get_json('data/raw/raw_meugov/antt_pedagio/')

def process_single_toll_file(file:str):
    file_path = os.path.join(INPUT_PATH, file)
    output_file = os.path.join(OUTPUT_PATH, f"{os.path.splitext(file)[0]}.csv")

    try:
        df = make_toll_df(file_path)
        Toll.validate(df, lazy=True)
        df.to_csv(output_file, sep=";", index=False)

    except pa.errors.SchemaErrors as exc:
        invalid_indices = exc.failure_cases["index"].unique()
        df_valid = df.drop(index=invalid_indices)

        if not df_valid.empty and len(invalid_indices)>0:
            logger.warning(f"{file}: {len(invalid_indices)} invalid rows removed")
            df_valid.to_csv(output_file, sep=";", index=False)
        else:
            logger.error(f"❗ {file}: All rows invalid. Moving file to {ERROR_PATH}.")
            shutil.copyfile(file_path, os.path.join(ERROR_PATH, file))

def run_toll_transformation_to_csv():
    """RODA O PROCESSAMENTO DO ARQUIVO CSV DO ANO MAIS RECENTE"""
    files = MeuGov.get_files_by_extension(INPUT_PATH, '.json')

    file_year_map = {
        f: MeuGov.extract_year_from_filename(f) for f in files if MeuGov.extract_year_from_filename(f) is not None
    }

    if not file_year_map:
        logger.warning("❗ No files with year --- Check extracted file.")
        return
    most_recent_year = max(file_year_map.values())
    
    recent_files = [f for f, y in file_year_map.items() if y == most_recent_year]
    for file in recent_files:
        process_single_toll_file(file)

def update_toll_consolidated_csv():
    """
    ATUALIZA O CSV CONSOLIDADO AO REMOVER REGISTROS DUPLICADOS COM BASE EM MES_ANO E ADICIONA OS NOMES REGISTROS DOS NOVOS MESES
    """
    if os.path.exists(CONSOLIDATED_PATH):
        df_consolidated = pd.read_csv(CONSOLIDATED_PATH, sep=";")
    else:
        df_consolidated = pd.DataFrame()

    processed_files = MeuGov.get_files_by_extension(OUTPUT_PATH, '.csv')

    for file in processed_files:
        file_path = os.path.join(OUTPUT_PATH, file)
        try:
            df_new = pd.read_csv(file_path, sep=";")

            if 'mes_ano' not in df_new.columns:
                logger.warning(f"'❗ Column 'mes_ano' not found in {file}. Skipping file.")
                continue

            mes_ano_novos = df_new['mes_ano'].unique()

            if not df_consolidated.empty:
                df_consolidated = df_consolidated[~df_consolidated['mes_ano'].isin(mes_ano_novos)]

            df_consolidated = pd.concat([df_consolidated, df_new], ignore_index=True)

        except Exception as e:
            logger.error(f"❗ TRANSFORMATION failed to process {file_path}: {e}")

    df_consolidated.to_csv(CONSOLIDATED_PATH, sep=";", index=False)
    logger.info(f"Update consolidated data: {CONSOLIDATED_PATH}")
    
def run_tolls_pipeline():
    """EXECUTA O PIPELINE COMPLETO."""
    logger.info("Initiating Toll Volume pipeline from ANTT..")
    extraction = run_tolls_extraction()
    if extraction:
        run_toll_transformation_to_csv()
        update_toll_consolidated_csv()
    else:
        logger.warning("⚠️  Pipeline execution stopped due failure on extraction")
    logger.info("✅ Toll volume ANTT pipeline completed!")
    logger.info("-"*100)

def run():
    recent_files = 'data/raw/raw_meugov/antt_pedagio'
    for file in os.listdir(recent_files):
        process_single_toll_file(file)

if __name__ == "__main__":
    # run_tolls_pipeline() 
    run()