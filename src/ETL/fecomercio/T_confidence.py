import pandas as pd 

pd.set_option('display.max_columns', None)

def indice_confianca_consumidor():
    file = 'data/raw/confidence/icc_link_download_202503.xlsx'
    df = pd.read_excel(file,header=1, sheet_name='SÉRIE')
    df.to_csv('data/processed/fecomercio/indice_confianca_consumidor.csv',index=False)

def indice_endividamento_inadimplencia():
    file = 'data/raw/confidence/peic_link_download_202503.xlsx'
    df = pd.read_excel(file,header=1, sheet_name='Série Histórica')
    df = df.iloc[:-1]
    df = df.rename(columns={'Unnamed: 0': 'data'})
    df = df.dropna(how='all')
    df = df.dropna(axis=1,how='all')
    df.columns = [col.lower() for col in df.columns]
    df.columns = [col.replace('\n', ' ') for col in df.columns]
    df = df.drop(columns=['endividadas.1','contas em atraso.1', 'não terão condições de pagar.1'])
    df.to_csv('data/processed/fecomercio/indice_endividamento_inadimplencia.csv',index=False)
    
def indice_confianca_empresario_comercial():
    file = 'data/raw/confidence/icec_link_download_202503.xlsx'
    df = pd.read_excel(file,header=0, sheet_name='Série Histórica Completa')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    exclude_rows = ['Empresas com até 50 Empregados','Empresas com mais de 50 Empregados','Semiduráveis','Não Duráveis','Duráveis']
    df = df[~df.apply(lambda row: row.astype(str).isin(exclude_rows).any(), axis=1)]
    df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
    df = df.dropna(subset=['valor_indice'])
    
    df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df = df.dropna(subset=['mes'])
    df.to_csv('data/processed/fecomercio/indice_confianca_empresario_comercial.csv',index=False)
    return df 

def estoques():
    file = 'data/raw/confidence/iestoques/estoques_link_download_202503.xlsx'
    df = pd.read_excel(file,header=0, sheet_name='IE - Série Histórica')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
    df = df.dropna(subset=['valor_indice'])
    df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df = df.dropna(subset=['mes'])
    df.to_csv('data/processed/fecomercio/indice_confianca_empresario_comercial.csv',index=False)
    return df 

def indice_expansao_comercio_sp():
    file = 'data/raw/confidence/iec/iec_link_download_202503.xlsx'
    df = pd.read_excel(file,header=0, sheet_name='IEC - Série Histórica')
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
    df = df.dropna(subset=['valor_indice'])
    df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
    df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
    df = df.dropna(subset=['mes'])
    df.to_csv('data/processed/fecomercio/indice_expansao_comercio_sp.csv',index=False)
    return df 

def pesquisa_conjuntural_comercio_varejista():
    file = 'data/raw/confidence/pccv/pccv_historico_precos_rea_is_03_2025_ref_01_2025.xlsx'
    df = pd.read_excel(file,header=15)
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='valor')
    df = df.rename(columns={df.columns[0]: 'comercio'})
    df.to_csv('data/processed/fecomercio/pesquisa_conjuntural_comercio_varejista.csv',index=False)
    
    return df 

def indice_custo_vida():
    file = 'data/raw/confidence/cvcs/cvcs_link_download_fev25.xlsx'
    df = pd.read_excel(file,header=0, sheet_name='Série Histórica')
    df = df.iloc[:-3]
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='custo_de_vida')
    df = df.drop(df.columns[0],axis=1)
    df.to_csv('data/processed/fecomercio/custo_de_vida.csv',index=False)
    return df 

def indice_preco_varejo():
    file = 'data/raw/confidence/ipv_link_download_fev25.xlsx'
    df = pd.read_excel(file,header=1, sheet_name='Série Histórica')
    df = df.iloc[:-3]
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='custo_de_vida')
    df = df.drop(df.columns[0],axis=1)
    # df.to_csv('data/processed/fecomercio/indice_preco_varejo.csv',index=False)
    return df 

def indice_preco_servicos():
    file = 'data/raw/confidence/ips/ips_link_download_fev25.xlsx'
    df = pd.read_excel(file,header=1, sheet_name='Série Histórica')
    df = df.iloc[:-3]
    df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='ips')
    df = df.drop(df.columns[0],axis=1)
    df['ips'] = pd.to_numeric(df['ips'], errors='coerce')
    df = df.dropna(subset=['ips'])
    df.to_csv('data/processed/fecomercio/indice_preco_servicos.csv',index=False)
    return df

def run():
    indice_confianca_consumidor()
    indice_endividamento_inadimplencia()
    indice_confianca_empresario_comercial()
    estoques()
    indice_expansao_comercio_sp()
    pesquisa_conjuntural_comercio_varejista()
    indice_custo_vida()
    indice_preco_varejo()
    indice_preco_servicos()