import pandas as pd 

from src.ETL.meugov.E_antt import run_antt_extraction
from src.ETL.meugov.E_energy_fuels import run_energy_and_fuels_extractions
from src.ETL.investidor_10.E_bitcoin import run_bitcoin_extracts
from src.ETL.infomoney.E_future_interest import run_future_interest_extractions
from src.ETL.bcb.E_m2 import run_m2_extract
from src.ETL.fecomercio.E_fecomercio import run_fecomercio_extracts



def run_extraction():

    run_antt_extraction() # OK
    run_energy_and_fuels_extractions() # OK
    run_bitcoin_extracts() # OK
    run_future_interest_extractions() # OK
    run_m2_extract() #OK
    run_fecomercio_extracts() # OK

run_extraction()