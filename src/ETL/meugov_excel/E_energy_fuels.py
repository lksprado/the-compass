import requests
import os

def download_excel_energia():
    url = "https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/dados-abertos/Documents/Dados_abertos_Consumo_Mensal.xlsx"
    response = requests.get(url)
    response.raise_for_status()  # Verifica se houve erro na requisição

    # Define o caminho onde o arquivo será salvo
    save_dir = 'data/raw/energy/'
    save_path = os.path.join(save_dir, 'Dados_abertos_Consumo_Mensal.xlsx')
    
    # Cria o diretório se não existir
    os.makedirs(save_dir, exist_ok=True)
    
    # Salva o arquivo Excel bruto
    with open(save_path, 'wb') as file:
        file.write(response.content)
    
    print(f"Arquivo baixado e salvo em: {os.path.abspath(save_path)}")

def download_excel_combustivel():
    url = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-brasil-desde-jan2013.xlsx"
    response = requests.get(url)
    response.raise_for_status()  # Verifica se houve erro na requisição

    # Define o caminho onde o arquivo será salvo
    save_dir = 'data/raw/fuel/'
    save_path = os.path.join(save_dir, 'precos_combustiveis.xlsx')
    
    # Cria o diretório se não existir
    os.makedirs(save_dir, exist_ok=True)
    
    # Salva o arquivo Excel bruto
    with open(save_path, 'wb') as file:
        file.write(response.content)
    
    print(f"Arquivo baixado e salvo em: {os.path.abspath(save_path)}")


def run_energia_combustiveis():
    download_excel_energia()
    download_excel_combustivel()
    
