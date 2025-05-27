import pandas as pd 
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ETL.meugov.pipeline_energy import run_energy_pipeline
from src.ETL.meugov.pipeline_fuels import run_fuels_pipeline
from src.ETL.meugov.pipeline_railways import run_railway_pipeline
from src.ETL.meugov.pipeline_tolls import run_tolls_pipeline
from src.ETL.investidor_10.pipeline_bitcoin import run_bitcoin_pipeline
from src.ETL.fecomercio.pipeline_fecomercio import run_fecomercio_pipeline
from src.ETL.infomoney.pipeline_infomoney import run_infomoney_pipeline


if __name__ == '__main__':
    run_energy_pipeline()
    run_fuels_pipeline()
    run_railway_pipeline()
    run_tolls_pipeline()
    run_bitcoin_pipeline()
    run_fecomercio_pipeline()
    run_infomoney_pipeline()
    
    