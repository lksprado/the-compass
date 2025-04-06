import pandas as pd 

df = pd.read_csv('data/processed/antt_ferrovias/railway_table.csv',sep=';')

print(df['mercadoria_antt'].unique())
