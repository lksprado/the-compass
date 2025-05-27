import requests
import os
from utils.logger import get_logger
logger = get_logger(__name__)


def get_excel(url:str,filename_extension:str,output_path:str):
    """EXTRAI EXCEL DO OFFICE 365"""
    os.makedirs(output_path, exist_ok=True)
    save_path = os.path.join(output_path, filename_extension)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  
        with open(save_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"Raw file retrieved succesfuly! Saved: {save_path}")
        return True 
    except Exception as err :
        logger.error(f"🚫 EXTRACTION failed to retrieve json: {err}")
        return False