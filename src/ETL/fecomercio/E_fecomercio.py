from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import os
import subprocess
import zipfile
import shutil 
import requests 
import logging

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/E_fecomercio.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

CHROMEDRIVER_DIR = "/home/lucas/.cache/selenium/chromedriver/linux64"
CHROMEDRIVER_PATH = os.path.join(CHROMEDRIVER_DIR, "chromedriver")

def get_local_chrome_version():
    """VERIFICA QUAL VERSAO DO GOOGLE CHROME ATUAL ATRAVES DO RETORNO EM LISTA DA VERSAO E DEFINIDA PELA VERSAO MAIOR NO PRIMEIRO ITEM DA LISTA"""
    output = subprocess.check_output([CHROMEDRIVER_PATH, "--version"]).decode("utf-8")
    version = output.strip().split()[1]
    major_version = version.split(".")[0]
    return int(major_version)

def get_latest_release_version():
    url = 'https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions.json'
    
    response = requests.get(url)
    data = response.json()
    
    stable_version = data['channels']['Stable']['version']
    stable_version_short = stable_version.split(".")[0]
    return int(stable_version_short), stable_version

def check_versions(local:int,release:tuple):
    release_major = release[0]
    release_number = release[1]
    if release_major > local:

        base_url = f"https://storage.googleapis.com/chrome-for-testing-public/{release_number}/linux64/chromedriver-linux64.zip"

        print(f"Baixando ChromeDriver versão {release}...")
        zip_path = "/tmp/chromedriver.zip"
        r = requests.get(base_url, stream=True)
        with open(zip_path, 'wb') as f:
            f.write(r.content)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall("/tmp/chromedriver_extracted")

        os.makedirs(CHROMEDRIVER_DIR, exist_ok=True)
        shutil.move("/tmp/chromedriver_extracted/chromedriver-linux64/chromedriver", CHROMEDRIVER_PATH)
        os.chmod(CHROMEDRIVER_PATH, 0o755)
        print(f"ChromeDriver {release} atualizado com sucesso em {CHROMEDRIVER_PATH}")
    else:
        print("Chromedriver já está atualizado")


def get_indices(urls:list):
    chromedriver_path = r"/home/lucas/.cache/selenium/chromedriver/linux64/chromedriver"

    download_dir = "/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    chrome_options = Options()
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument("--disable-gpu")  
    chrome_options.add_argument("--no-sandbox")  
    chrome_options.add_argument("--disable-dev-shm-usage") 

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(chromedriver_path)

    driver = webdriver.Chrome(service=service, options=chrome_options)  # Passar chrome_options
    
    for url in urls:
        try:
            print(f"Requesting {url}...")
            driver.get(url)
            time.sleep(3)
            download_link = driver.find_element(
                By.XPATH, 
                "//a[contains(@class, 'download')]"
            )
            
            file_url = download_link.get_attribute("href")
            file_name = file_url.split("/")[-1]  
            
            download_link.click()
            
            timeout = 10  
            start_time = time.time()
            while time.time() - start_time < timeout:
                if any(f.endswith(".xlsx") for f in os.listdir(download_dir)):
                    print(f"Data retrieved succesfuly! {file_name}")
                    break
                time.sleep(1)
            else:
                print("Wait timing expired. Download couldn't be finished, increase timeout.")

        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")

    driver.quit()

def run_fecomercio_extracts():
    print("Running Fecomercio extracts")
    local_version = get_local_chrome_version()
    release_version = get_latest_release_version()
    check_versions(local_version,release_version)
    urls = [
        'https://www.fecomercio.com.br/pesquisas/indice/icc', # OK
        # 'https://www.fecomercio.com.br/pesquisas/indice/icf', ## Erro 404 // SEM SERIE HISTORICA
        'https://www.fecomercio.com.br/pesquisas/indice/peic', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/icec', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/ie', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/iec', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/pccv', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/cvcs', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/ipv', # OK
        'https://www.fecomercio.com.br/pesquisas/indice/ips', # OK
        # 'https://www.fecomercio.com.br/pesquisas/indice/pesp-servicos', # Erro 404
        # 'https://www.fecomercio.com.br/pesquisas/indice/pesp-comercio', # Erro 404
        # 'https://www.fecomercio.com.br/pesquisas/indice/pcss', # Erro 404
        # 'https://www.fecomercio.com.br/pesquisas/indice/imat', # Desatualizado
        # 'https://www.fecomercio.com.br/pesquisas/indice/lvc', # Desatualizado
        # 'https://www.fecomercio.com.br/pesquisas/indice/ftn' # Desatualizado
    ]
    get_indices(urls)
    print("Fecomercio extracts done!")
    print("_"*20)
    
if __name__ == '__main__':
    run_fecomercio_extracts()