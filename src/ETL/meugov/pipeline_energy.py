from src.ETL.meugov.E_energy_fuels import *
from src.ETL.meugov.T_energy_fuels import *
from utils.logger import get_logger
logger = get_logger(__name__)

INPUT_PATH = 'data/raw/raw_meugov/energy/consumo_energia_eletrica_nacional.xlsx'
OUTPUT_PATH = 'data/processed/energy'


def run_energy_pipeline():
    url_energy = "https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/dados-abertos/Documents/Dados_abertos_Consumo_Mensal.xlsx"
    logger.info("Initiating Energy Usage pipeline from EPE..")
    extraction = get_excel(url_energy,'consumo_energia_eletrica_nacional.xlsx','data/raw/raw_meugov/energy')
    if extraction:
        make_energy_df('CONSUMO E NUMCONS SAM UF','consumo_uf',INPUT_PATH,OUTPUT_PATH)
        make_energy_df('SETOR INDUSTRIAL POR UF','consumo_setor_industrial_uf',INPUT_PATH,OUTPUT_PATH)
    else:
        logger.warning("⚠️  Pipeline execution stopped due failure on extraction")
    logger.info("✅ Energy Consumption pipeline completed!")
    logger.info("-"*100)

if __name__ == '__main__':
    run_energy_pipeline()