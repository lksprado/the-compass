from dotenv import load_dotenv
import requests 
import os 
import json 
import re
from utils.logger import get_logger
from typing import List, Dict

logger = get_logger(__name__)
load_dotenv()

class MeuGov():
    def __init__(self,url):
        self.url = url
        self.token = os.getenv("MEUGOV_TOKEN")
        self.headers = {
        'accept': 'application/json',
        'chave-api-dados-abertos': self.token
        }

    def get_json(self, output_path):
        url = self.url
        headers = self.headers
        filename = url.split('/')[-1]
        output_file_path = f'{output_path}{filename}'
        os.makedirs(os.path.dirname(output_file_path),exist_ok=True)
        
        try:
            response = requests.get(url,headers=headers,timeout=10)
            response.raise_for_status()
            data = response.json()
            with open(output_file_path, 'w') as f:
                json.dump(data,f,indent=4)
            logger.info(f'Raw file retrieved succesfuly! Saved: {output_file_path}')
            logger.info("-"*50)
            return True
        except Exception as err :
            logger.error(f"🚫 EXTRACTION failed to retrieve json: {err}")
            return False

    @staticmethod
    def get_files_by_extension(directory: str, extension: str) -> List[str]:
        """LISTA CADA ARQUIVO NO DIRETORIO DE ACORDO COM A EXTENSAO DESEJADA."""
        return [f for f in os.listdir(directory) if f.endswith(extension)]

    @staticmethod
    def extract_year_from_filename(filename: str) -> int:
        """EXTRAI O ANO DE CADA NOME DE ARQUIVO."""
        match = re.search(r'(20\d{2})', filename)
        return int(match.group(1)) if match else None
        




# https://dados.gov.br/swagger-ui/index.html#/Conjuntos%20de%20dados/detalhar_2


# REQUEST URL CÓDIGO DO CONJUNTO DE DADOS 
# https://dados.gov.br/dados/api/publico/conjuntos-dados?nomeConjuntoDados=sistema-de-acompanhamento-do-desempenho-operacional-das-concessionarias-siade&dadosAbertos=true&isPrivado=false&pagina=1

# curl
"""
curl -X 'GET' \
'https://dados.gov.br/dados/api/publico/conjuntos-dados?nomeConjuntoDados=sistema-de-acompanhamento-do-desempenho-operacional-das-concessionarias-siade&dadosAbertos=true&isPrivado=false&pagina=1' \
-H 'accept: application/json' \
-H 'chave-api-dados-abertos: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIwWTRGYjZBbnF1RFB0MEloZU02Q21oeVo0d3hxbkZQeGsyRDNzaU1rYUUteGpTd2I0WTBOeTVsLU9JamdmM0diTkVaUjRMLXNyaVpFMEdhQiIsImlhdCI6MTc0NDQwNDk1N30.wi_FDyZNdHH_L8VXvxBnH5RzGJ0yqPVAlqtxVG4xk0U'
"""

# REQUEST URL DE TODOS ARQUIVOS
# https://dados.gov.br/dados/api/publico/conjuntos-dados/438a5184-09db-49a3-88c8-0bad418b4409
# CURL 
"""
curl -X 'GET' \
'https://dados.gov.br/dados/api/publico/conjuntos-dados/438a5184-09db-49a3-88c8-0bad418b4409' \
-H 'accept: application/json' \
-H 'chave-api-dados-abertos: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIwWTRGYjZBbnF1RFB0MEloZU02Q21oeVo0d3hxbkZQeGsyRDNzaU1rYUUteGpTd2I0WTBOeTVsLU9JamdmM0diTkVaUjRMLXNyaVpFMEdhQiIsImlhdCI6MTc0NDQwNDk1N30.wi_FDyZNdHH_L8VXvxBnH5RzGJ0yqPVAlqtxVG4xk0U'
"""
# LINK 2025
"""
{
    "dataCatalogacao": "28/02/2025",
    "dataUltimaAtualizacaoArquivo": "28/02/2025",
    "link": "https://dados.antt.gov.br/dataset/438a5184-09db-49a3-88c8-0bad418b4409/resource/fecf6b19-6e91-42d1-baf0-ee64b8a5d246/download/producao_origem_destino_2025.json",
    "formato": "JSON",
    "quantidadeDownloads": null,
    "idConjuntoDados": "438a5184-09db-49a3-88c8-0bad418b4409",
    "numOrdem": null,
    "tipo": null,
    "titulo": "Produção Origem Destino 2025",
    "nomeArquivo": null,
    "descricao": "Histórico da movimentação mensal de cargas nas ferrovias federais concedidas.",
    "tamanho": 1598186,
    "id": "fecf6b19-6e91-42d1-baf0-ee64b8a5d246"
}
"""