from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import os
import time
from utils.logger import get_logger

logger = get_logger(__name__)



def run_m2_extraction():
    chromedriver_path = "/home/lucas/.cache/selenium/chromedriver/linux64/chromedriver"
    download_dir = "/media/lucas/Files/2.Projetos/the-compass/data/'data/raw/raw_monetary/'"
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
    try:
        # URL do Banco Central com a série de M2 ou página onde o botão de download aparece
        url = "https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries"

        driver.get(url)

        # A partir daqui, você pode localizar o botão de download ou clicar via Selenium
        # driver.find_element_by_xpath("...").click()

        # Aguarde o download
        time.sleep(5)

    finally:
        driver.quit()


if __name__ == '__main__':
    run_m2_extraction()