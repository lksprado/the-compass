import pandas as pd 

from src.ETL.meugov.E_meugov import run_antt_extraction
from src.ETL.meugov_excel.E_energy_fuels import run_energia_combustiveis
from src.ETL.investidor_10.E_bitcoin import bitcoin
from src.ETL.infomoney.E_future_interest import run_interest
from src.ETL.bcb.E_m2 import download_m2_csv
from src.ETL.fecomercio.E_fecomercio import run



def run_extraction():
    print("Running all")
    run_antt_extraction()
    print("ANTT done")
    run_energia_combustiveis()
    print("Energia e Combustíveis done")
    bitcoin()
    print("Bitcoin done")
    run_interest()
    print("Juros done")
    download_m2_csv()
    print("m2 done")
    run()
    print("Fecomercio done")
    
run_extraction()