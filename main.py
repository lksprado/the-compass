import pandas as pd 
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ETL.meugov.E_antt import run_antt_extraction
from src.ETL.meugov.E_energy_fuels import run_energy_and_fuels_extractions
from src.ETL.investidor_10.E_bitcoin import run_bitcoin_extracts
from src.ETL.infomoney.E_infomoney import run_future_interest_extractions
from src.ETL.bcb.E_m2 import run_m2_extraction
from src.ETL.fecomercio.E_fecomercio import run_fecomercio_extractions
from src.ETL.meugov.T_antt import run_antt_transformations
from src.ETL.meugov.T_energy_fuels import run_energy_fuel_transformations
from src.ETL.investidor_10.T_bitcoin import run_bitcoin_transformations
from src.ETL.infomoney.T_infomoney import run_future_interest_transformations
# from src.ETL.bcb.T_m2 import run_m2_transformations
from src.ETL.fecomercio.T_fecomercio import run_fecormecio_transformations



def run_extraction():
    try:
        run_antt_extraction() # OK
    except Exception as e :
        print(f"ANTT Extraction Failed -- {e}")
        
    try:    
        run_energy_and_fuels_extractions() # OK
    except Exception as e :
        print(f"Energy and Fuel Extraction Failed -- {e}")
    
    try:
        run_bitcoin_extracts() # OK
    except Exception as e :
        print(f"Bitcoin Extraction Failed -- {e}")
        
    try:
        run_future_interest_extractions() # OK
    except Exception as e :
        print(f"Future Interest Extraction Failed -- {e}")
        
    try:
        run_m2_extraction() #OK
    except Exception as e :
        print(f"M2 Extraction Failed -- {e}")
        
    try:    
        run_fecomercio_extractions() # OK
    except Exception as e :
        print(f"Fecomercio Extraction Failed -- {e}")

def run_transformation():
    try:
        run_antt_transformations() # OK
    except Exception as e :
        print(f"ANTT transformation Failed -- {e}")
        
    try:    
        run_energy_fuel_transformations() # OK
    except Exception as e :
        print(f"Energy and Fuel transformation Failed -- {e}")
    
    try:
        run_bitcoin_transformations() # OK
    except Exception as e :
        print(f"Bitcoin transformation Failed -- {e}")
        
    try:
        run_future_interest_transformations() # OK
    except Exception as e :
        print(f"Future Interest transformation Failed -- {e}")
        
    # try:
    #     run_m2_transformations() #OK
    # except Exception as e :
    #     print(f"M2 transformation Failed -- {e}")
        
    try:    
        run_fecormecio_transformations() # OK
    except Exception as e :
        print(f"Fecomercio transformation Failed -- {e}")

def gather_all():
    def railway():
        df = pd.read_csv('data/processed/antt_ferrovias/railway_table.csv',sep=';')
        df = df.groupby("mes_ano")["tu"].sum().reset_index()
        df = df.rename(columns={'mes_ano':'mes','tu':'ferrovia_volume_tonelada_util'})
        return df 
    
    def tolls():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/antt_pedagio/toll_table.csv',sep=';')
        df["mes_ano"] = pd.to_datetime(df["mes_ano"])
        df["mes_ano"] = df["mes_ano"].dt.to_period("M")
        df = df.groupby("mes_ano")["volume_total"].sum().reset_index()
        df["mes"] = df["mes_ano"].dt.to_timestamp().dt.strftime("%Y-%m-%d")
        df = df.drop(columns=['mes_ano'])
        df = df.rename(columns={'volume_total':'pedagios_volume'})
        return df
    
    def energy():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/energy/consumo_por_uf.csv',sep=';')
        df = df.groupby('data')[['consumo','consumidores']].sum().reset_index()
        df = df.rename(columns={'data':'mes'})
        return df 


    def bitcoin():
        df = pd.read_csv('data/processed/bitcoin/bitcoin.csv', sep=';')
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["mes"] = df["created_at"].dt.to_period("M")
        df = df.sort_values("created_at").groupby("mes").tail(1)
        df["created_at"] = df["mes"].dt.to_timestamp().dt.strftime("%Y-%m-%d")
        df = df.drop(columns=["mes"]).reset_index(drop=True)
        df = df.rename(columns={'created_at':'mes','dolar_price':'bitcoin_usd','brl_price':'bitcoin_brl'})          
        return df
    
    # def m2():
    #     df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_monetary/m2_supply.csv', sep=';')
    #     return df 
    
    df_railway = railway()
    
    df_tolls = tolls()

    df_energy = energy()

    df_bitcoin = bitcoin()

    df_custo_vida =  custo_de_vida()
    
    df_icc =  indice_confianca_consumidor()

    df_icec =  indice_confianca_empresario_comercial()
    
    df_peic =   indice_endividamente_inadimplencia()
    
    df_iecsp =  indice_expansao_comercio_sp()
    
    df_ips = indice_preco_servicos()

    df_ipv = indice_preco_varejo()

    df_comercio =  indice_conjuntural_comercio()
    
    # df_m2 = m2() 
    
    df = df_icc.merge(df_icec, on='mes',how='outer') \
                .merge(df_peic,on='mes',how='outer') \
                .merge(df_iecsp,on='mes',how='outer') \
                .merge(df_ips,on='mes',how='outer') \
                .merge(df_ipv,on='mes',how='outer') \
                .merge(df_comercio,on='mes',how='outer') \
                .merge(df_railway,on='mes',how='outer') \
                .merge(df_tolls,on='mes',how='outer') \
                .merge(df_energy,on='mes',how='outer') \
                .merge(df_bitcoin,on='mes',how='outer') \
                .merge(df_custo_vida,on='mes',how='outer') \
                # .merge(df_m2,on='mes',how='outer')
    
    df.to_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/final_df.csv',index=False,sep=';') 

def main():
    # run_extraction()
    # run_transformation()
    gather_all()
    
if __name__ == '__main__':
    main()
