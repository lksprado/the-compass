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


def gather_all():
    def railway():
        df = pd.read_csv('data/processed/antt_ferrovias/railway_table.csv',sep=';')
        df = df.groupby("mes_ano")["tu"].sum().reset_index()
        df = df.rename(columns={'tu':'ferrovia_volume_tonelada_util'})
        return df 
    
    def tolls():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/antt_pedagio/toll_table.csv',sep=';')
        df['mes_ano'] = pd.to_datetime(df['mes_ano'])
        df['mes_ano'] = df['mes_ano'].dt.to_period('M').dt.to_timestamp().dt.strftime("%Y-%m-%d")
        df = df.groupby("mes_ano")["volume_total"].sum().reset_index()
        df = df.rename(columns={'volume_total':'pedagios_volume'})
        return df
    
    def energy():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/energy/consumo_uf.csv',sep=';')
        df = df.groupby('mes_ano')[['consumo_mwh','consumidores']].sum().reset_index()
        return df 
    
    def bitcoin():
        df = pd.read_csv('data/processed/bitcoin/bitcoin.csv', sep=';')
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["mes"] = df["created_at"].dt.to_period("M")
        df = df.sort_values("created_at").groupby("mes").tail(1)
        df["created_at"] = df["mes"].dt.to_timestamp().dt.strftime("%Y-%m-%d")
        df = df.drop(columns=["mes"]).reset_index(drop=True)
        df = df.rename(columns={'created_at':'mes_ano','dolar_price':'bitcoin_usd','brl_price':'bitcoin_brl'})          
        return df


    def custo_de_vida():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/custo_de_vida.csv', sep=';')
        return df 
    
    def indice_confianca_consumidor():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/indice_confianca_consumidor.csv', sep=';')
        return df 

    def indice_confianca_empresario_comercial():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/indice_confianca_empresario_comercial.csv', sep=';')
        return df 
    
    def indice_endividamento_inadimplencia():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/indice_endividamento_inadimplencia.csv', sep=';')
        return df 
    
    def indice_expansao_comercio_sp():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/indice_expansao_comercio_sp.csv', sep=';')
        return df 
    
    def indice_preco_servicos():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/indice_preco_servicos.csv', sep=';')
        return df 

    def indice_preco_varejo():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/indice_preco_varejo.csv', sep=';')
        return df 

    def indice_conjuntural_comercio():
        df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/fecomercio/pesquisa_conjuntural_comercio_varejista.csv', sep=';')
        return df 
    
    
    df_railway = railway()
    
    df_tolls = tolls()

    df_energy = energy()

    df_bitcoin = bitcoin()

    df_custo_vida =  custo_de_vida()
    
    df_icc =  indice_confianca_consumidor()

    df_icec =  indice_confianca_empresario_comercial()
    
    df_peic =   indice_endividamento_inadimplencia()
    
    df_iecsp =  indice_expansao_comercio_sp()
    
    df_ips = indice_preco_servicos()

    df_ipv = indice_preco_varejo()

    df_comercio =  indice_conjuntural_comercio()
    
    df = df_icc.merge(df_icec, on='mes_ano',how='outer') \
                .merge(df_peic,on='mes_ano',how='outer') \
                .merge(df_iecsp,on='mes_ano',how='outer') \
                .merge(df_ips,on='mes_ano',how='outer') \
                .merge(df_ipv,on='mes_ano',how='outer') \
                .merge(df_comercio,on='mes_ano',how='outer') \
                .merge(df_railway,on='mes_ano',how='outer') \
                .merge(df_tolls,on='mes_ano',how='outer') \
                .merge(df_energy,on='mes_ano',how='outer') \
                .merge(df_bitcoin,on='mes_ano',how='outer') \
                .merge(df_custo_vida,on='mes_ano',how='outer')

    return df 


if __name__ == '__main__':
    run_energy_pipeline()
    run_fuels_pipeline()
    run_railway_pipeline()
    run_tolls_pipeline()
    run_bitcoin_pipeline()
    run_fecomercio_pipeline()
    run_infomoney_pipeline()
    final = gather_all()
    final.to_csv('/media/lucas/Files/2.Projetos/the-compass/final_df.csv',sep=';',index=False)