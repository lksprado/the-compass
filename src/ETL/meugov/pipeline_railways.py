from src.ETL.meugov.E_antt import MeuGov
from src.ETL.meugov.T_antt import *
from utils.logger import get_logger

logger = get_logger(__name__)

INPUT_PATH = 'data/raw/raw_meugov/antt_ferrovias'
OUTPUT_PATH = 'data/processed/antt_ferrovias/arquivos'
ERROR_PATH = 'data/processed/errors'
CONSOLIDATED_PATH = 'data/processed/antt_ferrovias/railway_table.csv'

def run_railway_extraction():
    print("Running ANTT extracts")
    railway_url = 'https://dados.antt.gov.br/dataset/438a5184-09db-49a3-88c8-0bad418b4409/resource/fecf6b19-6e91-42d1-baf0-ee64b8a5d246/download/producao_origem_destino_2025.json'
    railways = MeuGov(railway_url)
    railways.get_json('data/raw/raw_meugov/antt_ferrovias/')

def process_single_railway_file(file: str):
    """PROCESSA O ARQUIVO JSON E SALVA EM CSV."""
    file_path = os.path.join(INPUT_PATH, file)
    output_file = os.path.join(OUTPUT_PATH, f"{os.path.splitext(file)[0]}.csv")

    try:
        df = make_railway_df(file_path)
        Railway.validate(df, lazy=True)
        df.to_csv(output_file, sep=";", index=False)

    except pa.errors.SchemaErrors as exc:
        invalid_indices = exc.failure_cases["index"].unique()
        df_valid = df.drop(index=invalid_indices)

        if not df_valid.empty:
            logger.warning(f"{file}: {len(invalid_indices)} invalid rows removed")
            df_valid.to_csv(output_file, sep=";", index=False)
        else:
            logger.error(f"❗ {file}: All rows invalid. Moving file to {ERROR_PATH}.")
            shutil.copyfile(file_path, os.path.join(ERROR_PATH, file))

def run_railway_transformation_to_csv():
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
        process_single_railway_file(file)


def update_railway_consolidated_csv():
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


def run_railway_pipeline():
    """EXECUTA O PIPELINE COMPLETO."""
    logger.info("Initiating Railway Cargo pipeline from ANTT...")
    extraction = run_railway_extraction()
    if extraction:
        run_railway_transformation_to_csv()
        update_railway_consolidated_csv()
    else:
        logger.warning("⚠️  Pipeline execution stopped due failure on extraction")
    logger.info("✅ Railway cargo ANTT pipeline completed!")
    logger.info("-"*100)

if __name__ == "__main__":
    run_railway_pipeline()