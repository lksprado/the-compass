import pandas as pd 

df = pd.read_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/antt_pedagio/toll_table.csv',sep=';')
df["mes_ano"] = pd.to_datetime(df["mes_ano"])
df["mes_ano"] = df["mes_ano"].dt.to_period("M")
df = df.groupby("mes_ano")["volume_total"].sum().reset_index()
df["mes"] = df["mes_ano"].dt.to_timestamp().dt.strftime("%Y-%m-%d")
df = df.drop(columns=['mes_ano'])
df = df.rename(columns={'volume_total':'pedagios_volume'})
df.to_csv('/media/lucas/Files/2.Projetos/the-compass/data/processed/tolls.csv',sep=';')   