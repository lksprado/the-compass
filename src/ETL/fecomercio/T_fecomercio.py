import pandas as pd 
import os 
import shutil 
pd.set_option('display.max_columns', None)

def file_mover():
    downloads_dir = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/' 
    file_list = os.listdir(downloads_dir)
    filenames = [item for item in file_list if item.endswith('.xlsx')] # cria nova lista
    destination = {
        'cvcs': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/cvcs',
        'estoques': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/iestoques',
        'icc': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/icc',
        'icec': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/icec',
        'iec': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/iec',
        'ipv': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/ipv',
        'pccv': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/pccv',
        'peic': '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/peic'
    }
    for file in filenames:
        indice = file.strip().split('_')[0]
        if indice in destination:
            dest_dir = destination[indice]
            for f in os.listdir(dest_dir):
                os.remove(os.path.join(dest_dir,f))
            
            shutil.move(os.path.join(downloads_dir, file), dest_dir)
            print(f"Arquivo {file} movido para {dest_dir}")


### alterar file para que leia o unico arquivo na pasta
def indice_confianca_consumidor():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/icc/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=1, sheet_name='SÉRIE')
    df.to_csv('data/processed/fecomercio/indice_confianca_consumidor.csv',index=False, sep=';')

def indice_endividamento_inadimplencia():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/peic/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica')
    df = df.iloc[:-1]
    df = df.rename(columns={'Unnamed: 0': 'data'})
    df = df.dropna(how='all')
    df = df.dropna(axis=1,how='all')
    df.columns = [col.lower() for col in df.columns]
    df.columns = [col.replace('\n', ' ') for col in df.columns]
    df = df.drop(columns=['endividadas.1','contas em atraso.1', 'não terão condições de pagar.1'])
    df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.date
    df.to_csv('data/processed/fecomercio/indice_endividamento_inadimplencia.csv',index=False, sep=';')
    
def indice_confianca_empresario_comercial():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/icec/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=0, sheet_name='Série Histórica Completa')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    exclude_rows = ['Empresas com até 50 Empregados','Empresas com mais de 50 Empregados','Semiduráveis','Não Duráveis','Duráveis']
    df = df[~df.apply(lambda row: row.astype(str).isin(exclude_rows).any(), axis=1)]
    df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
    df = df.dropna(subset=['valor_indice'])
    
    df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df = df.dropna(subset=['mes'])
    df.to_csv('data/processed/fecomercio/indice_confianca_empresario_comercial.csv',index=False, sep=';')
    return df 

def estoques():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/iestoques/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=0, sheet_name='IE - Série Histórica')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
    df = df.dropna(subset=['valor_indice'])
    df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df = df.dropna(subset=['mes'])
    df.to_csv('data/processed/fecomercio/indice_confianca_empresario_comercial.csv',index=False, sep=';')
    return df 

def indice_expansao_comercio_sp():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/iec/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=0, sheet_name='IEC - Série Histórica')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
    df = df.dropna(subset=['valor_indice'])
    df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df = df.dropna(subset=['mes'])
    df.to_csv('data/processed/fecomercio/indice_expansao_comercio_sp.csv',index=False, sep=';')
    return df 

def pesquisa_conjuntural_comercio_varejista():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/pccv/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=15)
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='valor')
    df = df.rename(columns={df.columns[0]: 'comercio'})
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df.to_csv('data/processed/fecomercio/pesquisa_conjuntural_comercio_varejista.csv',index=False, sep=';')
    
    return df 

def indice_custo_vida():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/cvcs/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=0, sheet_name='Série Histórica')
    df = df.iloc[:-3]
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='custo_de_vida')
    df = df.drop(df.columns[0],axis=1)
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df.to_csv('data/processed/fecomercio/custo_de_vida.csv',index=False, sep=';')
    return df 

def indice_preco_varejo():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/ipv/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica')
    df = df.iloc[:-3]
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='custo_de_vida')
    df = df.drop(df.columns[0],axis=1)
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df.to_csv('data/processed/fecomercio/indice_preco_varejo.csv',index=False, sep=';')
    return df 

def indice_preco_servicos():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/ips/'
    for file in os.listdir(folder):
        if file.endswith('.xlsx'):
            file_path = os.path.join(folder, file)
            break
    df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica')
    df = df.iloc[:-3]
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='ips')
    df = df.drop(df.columns[0],axis=1)
    df['ips'] = pd.to_numeric(df['ips'], errors='coerce')
    df = df.dropna(subset=['ips'])
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df.to_csv('data/processed/fecomercio/indice_preco_servicos.csv',index=False,sep=';')
    return df

def run():
    file_mover()
    indice_confianca_consumidor()
    indice_endividamento_inadimplencia()
    indice_confianca_empresario_comercial()
    estoques()
    indice_expansao_comercio_sp()
    pesquisa_conjuntural_comercio_varejista()
    indice_custo_vida()
    indice_preco_varejo()
    indice_preco_servicos()

if __name__ == '__main__':
    run()