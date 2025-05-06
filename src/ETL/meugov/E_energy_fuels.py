import requests
import os
import logging

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/E_energy_fuels.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_excel(url,output_path):
    os.makedirs(output_path, exist_ok=True)
    filename = url.split('/')[-1]
    save_path = os.path.join(output_path, filename)

    try:
        print(f"Requesting {url}...")
        response = requests.get(url)
        response.raise_for_status()  
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Data retrieved succesfuly! File saved: {save_path}")
    except requests.exceptions.RequestException as err :
        print("Error, check log for details")
        logger.error(err)

def run_energy_and_fuels_extractions():
    print("Running Energy and Fuel extracts")
    url_energy = "https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/dados-abertos/Documents/Dados_abertos_Consumo_Mensal.xlsx"
    get_excel(url_energy,'data/raw/raw_meugov/energy')
    url_fuels = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-brasil-desde-jan2013.xlsx"
    get_excel(url_fuels,'data/raw/raw_meugov/fuel')
    print("Energy and Fuel extracts done!")
    print("_"*20)