import os
# import sys
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import locale
import altair as alt
import pandas as pd
import streamlit as st
from datetime import datetime, date
from decimal import Decimal
import logging
from datetime import datetime
logging.basicConfig(level=logging.DEBUG)
from dotenv import load_dotenv
load_dotenv()

##################################################################################################################################
## CONFIGURACOES INICIAIS ########################################################################################################
st.set_page_config("BÚSSOLA", layout="wide", page_icon="📊")
st.header("BÚSSOLA")

locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
verde_escuro = "#07ED41"
verde_medio = "#39F969"
verde_claro = "#88FBA5"
vermelho_medio = "#ff5e5b"
vermelho_claro = "#D57270"
azul_medio = "#00cecb"
azul_claro = "#5cfffc"
azul_escuro = "#007a78"
rosa = "#fd3e81"
stroke = 2.5
##################################################################################################################################
## OBTER DATAFRAME PRINCIPAL #####################################################################################################

df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/final_df.csv',sep=';',parse_dates=['mes_ano'])
df['mes_ano'] = df['mes_ano'].to_period('M')

def linha_simples(
    df: pd.DataFrame, col_name: str, max_month, min_month, cor1: str, intervalo: str = "tras",numero:str = 'normal', title:str = None):
    """
    df: Algum dataframe
    col_name: Coluna numérica para analisar
    max_month: Máximo mês no final
    min_month: Mínimo mês no final
    intervalo: Passado ou Futuro
    cor1: Cor em destaque
    cor2: Cor secundária
    """
    col = col_name
    cor_1 = cor1
    if max_month is not None and min_month is not None:
        if intervalo == "tras":
            df_filtered = df[(df["MES"] <= max_month) & (df["MES"] >= min_month)]
        elif intervalo == "frente":
            df_filtered = df[(df["MES"] >= max_month) & (df["MES"] <= min_month)]
    else:
        # Caso max_month e min_month sejam None, usa o DataFrame completo
        df_filtered = df
    
    if numero == 'normal':
        df_filtered["Valor_formatado"] = df_filtered[f"{col}"].apply(
            lambda x: f"{x:,.0f}".replace(",", ".")
        )
    elif numero == 'porcentagem':
        df_filtered["Valor_formatado"] = df_filtered[f"{col}"].apply(
            lambda x: f"{x:,.1%}".replace(".", ",")
        )
    elif numero == 'decimal':
        df_filtered["Valor_formatado"] = df_filtered[f"{col}"].apply(
            lambda x: f"{x:,.2f}".replace(".", ",")
        )
    chart = (
        alt.Chart(df_filtered)
        .mark_line()
        .encode(
            x=alt.X(
                "MES_STR:N",
                title=None,
                sort=alt.SortField(field="date", order="ascending"),
                axis=alt.Axis(
                    ticks=True,
                    grid=False,
                    domain=True,
                    tickColor="gray",
                    domainColor="gray",
                    labelAngle=0,
                ),
            ),
            y=alt.Y(
                f"{col}:Q",
                title=None,
                axis=alt.Axis(
                    grid=False,
                    domain=True,
                    domainColor="gray",
                    labels=False,
                ),
            ),
            tooltip=[
                alt.Tooltip("MES_STR:N", title="MÊS"),
                alt.Tooltip("Valor_formatado:N", title=f"{col}")
            ],
            color=alt.value(f"{cor_1}"),
        )
        .properties(title=f"{title}")
    )
    text = chart.mark_text(
        align="center",
        baseline="bottom",
        dy=-10,
        fontWeight="bold",
    ).encode(text=alt.Text(f"Valor_formatado:N"))
    chart = chart + text
    return chart

def linha_simples_sem_rotulo(
    df: pd.DataFrame, col_date:str, col_name: str, cor1: str, intervalo: str = "tras", max_month=None, min_month = None, numero:str = 'normal', title:str = None):
    """
    df: Algum dataframe \n
    col_name: Coluna numérica para analisar \n
    max_month: Máximo mês no final \n
    min_month: Mínimo mês no final \n
    intervalo: Passado ou Futuro \n
    cor1: Cor em destaque
    """
    col = col_name
    cor_1 = cor1

    if min_month is not None:
        min_month = pd.to_datetime(min_month, format='%Y-%m')
    if max_month is not None:
        max_month = pd.to_datetime(max_month, format='%Y-%m')
    
    if max_month is not None and min_month is not None:
        if intervalo == "tras":
            df_filtered = df[(df[col_date] <= max_month) & (df[col_date] >= min_month)]
        elif intervalo == "frente":
            df_filtered = df[(df[col_date] >= max_month) & (df[col_date] <= min_month)]
    else:
        # Caso max_month e min_month sejam None, usa o DataFrame completo
        df_filtered = df
    
    if numero == 'normal':
        df_filtered["Valor_formatado"] = df_filtered[f"{col}"].apply(
            lambda x: f"{x:,.0f}".replace(",", ".")
        )
    elif numero == 'porcentagem':
        df_filtered["Valor_formatado"] = df_filtered[f"{col}"].apply(
            lambda x: f"{x:,.1%}".replace(".", ",")
        )
    elif numero == 'decimal':
        df_filtered["Valor_formatado"] = df_filtered[f"{col}"].apply(
            lambda x: f"{x:,.2f}".replace(".", ",")
        )
    chart = (
        alt.Chart(df_filtered)
        .mark_line()
        .encode(
            x=alt.X(
                f"{col_date}:N",
                title=None,
                sort=alt.SortField(field="date", order="ascending"),
                axis=alt.Axis(
                    ticks=True,
                    grid=False,
                    domain=True,
                    tickColor="gray",
                    domainColor="gray",
                    labelAngle=0,
                ),
            ),
            y=alt.Y(
                f"{col}:Q",
                title=None,
                axis=alt.Axis(
                    grid=False,
                    domain=True,
                    domainColor="gray",
                    labels=True,
                    ticks=True,
                    tickColor="gray",
                ),
            ),
            tooltip=[
                alt.Tooltip(f"{col_date}:N", title="MÊS"),
                alt.Tooltip("Valor_formatado:N", title=f"{col}")
            ],
            color=alt.value(f"{cor_1}"),
        )
        .properties(title=f"{title}")
    )
    return chart

chart = linha_simples_sem_rotulo(df=df,col_date='mes_ano',col_name='icc_indice_confianca_consumidor',cor1=vermelho_medio, title="ICC", min_month='2011-01',intervalo='tras')
st.altair_chart(chart, use_container_width=True)