from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import os


def get_indices(urls:list):
    # Configurar o caminho do chromedriver
    chromedriver_path = r"/home/lucas/.cache/selenium/chromedriver/linux64/chromedriver"

    # Configurar o diretório de download
    download_dir = "/media/lucas/Files/2.Projetos/the-compass/data/raw/confidence"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Configurar opções do Chrome
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Ativar modo headless
    # chrome_options.add_argument("--disable-gpu")  # Necessário em alguns sistemas para headless
    # chrome_options.add_argument("--no-sandbox")  # Recomendado para Linux em headless
    # chrome_options.add_argument("--disable-dev-shm-usage")  # Evita problemas em sistemas com pouca memória compartilhada

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(chromedriver_path)

    # Iniciar o navegador
    driver = webdriver.Chrome(service=service, options=chrome_options)  # Passar chrome_options
    
    for url in urls:
        try:
            # Acessar a URL
            driver.get(url)
            
            # Aguardar a página carregar
            time.sleep(3)
            
            # Encontrar o elemento de download (genérico para class="download")
            download_link = driver.find_element(
                By.XPATH, 
                "//a[contains(@class, 'download')]"
            )
            
            # Obter o nome do arquivo do href para referência
            file_url = download_link.get_attribute("href")
            file_name = file_url.split("/")[-1]  # Pega o nome do arquivo do URL
            
            # Clicar no link para iniciar o download
            download_link.click()
            
            # Aguardar o download completar
            timeout = 10  # Tempo máximo de espera em segundos
            start_time = time.time()
            while time.time() - start_time < timeout:
                if any(f.endswith(".xlsx") for f in os.listdir(download_dir)):
                    print(f"Download concluído {file_name}")
                    break
                time.sleep(1)
            else:
                print("Tempo de espera esgotado. O download pode não ter sido concluído.")

        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")

    driver.quit()
    print("Done!")


if __name__ == '__main__':
    urls = [
        'https://www.fecomercio.com.br/pesquisas/indice/icc',
        'https://www.fecomercio.com.br/pesquisas/indice/icf',
        'https://www.fecomercio.com.br/pesquisas/indice/peic',
        'https://www.fecomercio.com.br/pesquisas/indice/icec',
        'https://www.fecomercio.com.br/pesquisas/indice/ie',
        'https://www.fecomercio.com.br/pesquisas/indice/iec',
        'https://www.fecomercio.com.br/pesquisas/indice/pccv',
        'https://www.fecomercio.com.br/pesquisas/indice/cvcs',
        'https://www.fecomercio.com.br/pesquisas/indice/ipv',
        'https://www.fecomercio.com.br/pesquisas/indice/ips',
        'https://www.fecomercio.com.br/pesquisas/indice/pesp-servicos',
        'https://www.fecomercio.com.br/pesquisas/indice/pesp-comercio',
        'https://www.fecomercio.com.br/pesquisas/indice/pcss',
        'https://www.fecomercio.com.br/pesquisas/indice/imat',
        'https://www.fecomercio.com.br/pesquisas/indice/lvc',
        'https://www.fecomercio.com.br/pesquisas/indice/ftn'
    ]
    get_indices(urls)