import requests
import os

def download_excel():
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

if __name__ == '__main__':
    download_excel()