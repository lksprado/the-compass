from src.ETL.meugov.E_energy_fuels import *
from src.ETL.meugov.T_energy_fuels import *
from utils.logger import get_logger
logger = get_logger(__name__)


def run_energy_pipeline():
    url_energy = "https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/dados-abertos/Documents/Dados_abertos_Consumo_Mensal.xlsx"
    get_excel(url_energy,'data/raw/raw_meugov/energy')