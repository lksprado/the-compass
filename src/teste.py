import pandas as  pd 
# from ydata_profiling import ProfileReport
data = pd.read_csv('data/processed/antt_pedagio/toll_table.csv',sep=';', dtype={'categoria':str})

categoria = data['categoria'].unique()
categoria = pd.DataFrame(categoria)
categoria.to_csv('categoria.csv')

# profile = ProfileReport(data)
# profile.to_file("your_report.html")