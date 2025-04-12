import ipeadatapy as ip 

lists = ip.list_series()
lists.to_csv('docs/ipea_series.csv',sep=';')