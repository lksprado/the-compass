import requests
import os

def download_csv():
    url = "https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=downLoad"
    
    # String de cookies
    cookies_str = 'SGS/ConfiguracoesAmbiente=I/E/E; TS019aa75b=012e4f88b379961ff22fffc3e65a5b5ed34dc99862d6577ada8040800e6a678ad84388823a5fd372ed001dbf350286f3e113298a614958ab89f2a7134a8e15ea98d2c07e35d80982157b769614bd3ce78997889a363705d6c96a1657d2b51b3bcca3e053df9c43e435c421008c0650b71075c9e77f; _ga=GA1.3.1759995962.1742161653; JSESSIONID=0000Cx4Eu_1LalVja-wO2tSlR_E:1dr9acgcl; BIGipServer~was_p_as3~was_p~pool_was_443_p=1020268972.47873.0000; BIGipServer~olinda_as3~olinda_p~pool_dmz-vs-pwas_443_p=!5jznb9g6MTLifM3pFQofo1yKe5WLRJNIgFGn60KZnFHgqG9AyWOn/x2NZBkypoDERTrrl43sjgG0S2k=; TS012b7e98=012e4f88b358d6ee655cc0573759cadeaf3d56de245ba2b9a91ab9cb8df02867131fb940bf3bb8d66d3e058cd427d3d625b5bc96f415e9ccb503b2fbd848c2b9da7e170c29c6302f4b727c14649390cb30a6f45d7fca48d3c828163029bc38da929e8a459b3f87beae2811077495e33e05ca5b7162; BIGipServer~www3_p_as3~www3_p~pool_dmz-vs-pwas_443_p=!LckqOD/9Ouq56QfpFQofo1yKe5WLRO6S0lhljwdNUK513/mCSEZcGwIJOS02hebE4UV/ltEoRQ0xXyY='
    
    # Parsear a string de cookies em um dicionário
    cookies = {}
    for cookie in cookies_str.split('; '):
        if '=' in cookie:
            name, value = cookie.split('=', 1)  # Divide apenas na primeira ocorrência de '='
            cookies[name] = value
    
    # Faz a requisição com os cookies parseados
    response = requests.get(url, cookies=cookies)
    response.raise_for_status()  # Verifica se houve erro na requisição

    # Define o caminho onde o arquivo será salvo
    save_dir = 'data/raw/monetary/'
    save_path = os.path.join(save_dir, 'm2_supply.csv')
    
    # Cria o diretório se não existir
    os.makedirs(save_dir, exist_ok=True)
    
    # Salva o arquivo
    with open(save_path, 'wb') as file:
        file.write(response.content)
    
    print(f"Arquivo baixado e salvo em: {os.path.abspath(save_path)}")

if __name__ == '__main__':
    download_csv()