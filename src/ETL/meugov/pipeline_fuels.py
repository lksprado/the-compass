from src.ETL.meugov.E_energy_fuels import *
from src.ETL.meugov.T_energy_fuels import *
from utils.logger import get_logger
logger = get_logger(__name__)

INPUT_PATH = 'data/raw/raw_meugov/fuel/fuel_prices.xlsx'
OUTPUT_PATH = 'data/processed/energy'


def run_fuels_pipeline():
    url_fuel = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-brasil-desde-jan2013.xlsx"
    logger.info("Initiating Fuel Prices pipeline from ANP...")
    extraction = get_excel(url_fuel,'fuel_prices.xlsx','data/raw/raw_meugov/fuel')
    if extraction:
        make_fuel_df(INPUT_PATH,OUTPUT_PATH)
    else:
        logger.warning("⚠️  Pipeline execution stopped due failure on extraction")
    logger.info("✅ Fuel Prices pipeline completed!")
    logger.info("-"*100)

if __name__ == '__main__':
    run_fuels_pipeline()