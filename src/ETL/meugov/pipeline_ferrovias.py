from src.ETL.meugov.E_antt import MeuGov
from src.ETL.meugov.T_antt import *
from src.ETL.meugov.E_energy_fuels import *
from src.ETL.meugov.T_energy_fuels import *
import json
from datetime import datetime
from typing import List, Dict
import re 
from utils.logger import get_logger

logger = get_logger(__name__)

def run_railway_extraction():
    print("Running ANTT extracts")
    railway_url = 'https://dados.antt.gov.br/dataset/438a5184-09db-49a3-88c8-0bad418b4409/resource/fecf6b19-6e91-42d1-baf0-ee64b8a5d246/download/producao_origem_destino_2025.json'
    railways = MeuGov(railway_url)
    railways.get_json('data/raw/raw_meugov/antt_ferrovias/')

# Configurações de paths
INPUT_PATH = 'data/raw/raw_meugov/antt_ferrovias'
OUTPUT_PATH = 'data/processed/antt_ferrovias/arquivos'
ERROR_PATH = 'data/processed/errors'
CONSOLIDATED_PATH = 'data/processed/antt_ferrovias/railway_table.csv'


def get_files_by_extension(directory: str, extension: str) -> List[str]:
    """Lista arquivos com determinada extensão no diretório."""
    return [f for f in os.listdir(directory) if f.endswith(extension)]


def extract_year_from_filename(filename: str) -> int:
    """Extrai o ano de um nome de arquivo."""
    match = re.search(r'(20\d{2})', filename)
    return int(match.group(1)) if match else None


def run_railway_transformation_to_csv():
    """Processa arquivos JSON do ano mais recente e os transforma em CSVs validados."""
    files = get_files_by_extension(INPUT_PATH, '.json')

    file_year_map = {
        f: extract_year_from_filename(f) for f in files if extract_year_from_filename(f) is not None
    }

    if not file_year_map:
        logger.warning("Nenhum arquivo com ano identificado foi encontrado.")
        return

    most_recent_year = max(file_year_map.values())
    logger.info(f"Processando arquivos do ano mais recente: {most_recent_year}")

    recent_files = [f for f, y in file_year_map.items() if y == most_recent_year]

    for file in recent_files:
        process_single_railway_file(file)


def process_single_railway_file(file: str):
    """Processa e valida um único arquivo JSON, salvando como CSV."""
    file_path = os.path.join(INPUT_PATH, file)
    output_file = os.path.join(OUTPUT_PATH, f"{os.path.splitext(file)[0]}.csv")

    try:
        df = make_railway_df(file_path)
        df.columns = df.columns.str.lower()
        Railway.validate(df, lazy=True)
        df.to_csv(output_file, sep=";", index=False)
        logger.info(f"Arquivo processado com sucesso: {output_file}")

    except pa.errors.SchemaErrors as exc:
        invalid_indices = exc.failure_cases["index"].unique()
        df_valid = df.drop(index=invalid_indices)

        if not df_valid.empty:
            logger.warning(f"{file}: {len(invalid_indices)} linhas inválidas removidas.")
            df_valid.to_csv(output_file, sep=";", index=False)
        else:
            logger.error(f"{file}: Todas as linhas inválidas. Arquivo movido para {ERROR_PATH}.")
            shutil.copyfile(file_path, os.path.join(ERROR_PATH, file))


def update_railway_consolidated_csv():
    """
    Atualiza o CSV consolidado removendo registros duplicados com base em 'mes_ano'
    e adicionando novos registros dos arquivos processados.
    """
    if os.path.exists(CONSOLIDATED_PATH):
        df_consolidated = pd.read_csv(CONSOLIDATED_PATH, sep=";")
        logger.info(f"Arquivo consolidado carregado: {CONSOLIDATED_PATH}")
    else:
        df_consolidated = pd.DataFrame()
        logger.info("Nenhum arquivo consolidado encontrado. Criando novo.")

    processed_files = get_files_by_extension(OUTPUT_PATH, '.csv')

    for file in processed_files:
        file_path = os.path.join(OUTPUT_PATH, file)
        try:
            df_new = pd.read_csv(file_path, sep=";")

            if 'mes_ano' not in df_new.columns:
                logger.warning(f"'mes_ano' não encontrado em {file}. Arquivo ignorado.")
                continue

            mes_ano_novos = df_new['mes_ano'].unique()

            if not df_consolidated.empty:
                df_consolidated = df_consolidated[~df_consolidated['mes_ano'].isin(mes_ano_novos)]

            df_consolidated = pd.concat([df_consolidated, df_new], ignore_index=True)

        except Exception as e:
            logger.error(f"Erro ao processar {file_path}: {e}")

    df_consolidated.to_csv(CONSOLIDATED_PATH, sep=";", index=False)
    logger.info(f"Arquivo consolidado atualizado: {CONSOLIDATED_PATH}")


def full_pipeline():
    """Executa a pipeline completa: transformação + atualização do consolidado."""
    logger.info("Iniciando pipeline de ferrovias ANTT...")
    extraction = run_railway_extraction()
    if not extraction:
        logger.warning("Pipeline execution stopped due failure on extraction")
        exit(1)
    run_railway_transformation_to_csv()
    update_railway_consolidated_csv()
    logger.info("✅ Pipeline de ferrovias ANTT concluída com sucesso.")
    logger.info("-"*50)

if __name__ == "__main__":
    full_pipeline()