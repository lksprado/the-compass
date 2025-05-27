import pandas as pd 
import shutil 
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from utils.logger import get_logger
from unidecode import unidecode 

pd.set_option('display.max_columns', None)

logger = get_logger(__name__)

def file_mover():
    """MOVER ARQUIVOS DO DIRETORIO PAI PARA OS DIRETORIOS FILHOS"""
    downloads_dir = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/' 
    file_list = os.listdir(downloads_dir)
    filenames = [item for item in file_list if item.endswith('.xlsx')] # cria nova lista
    destination = {
        'cvcs': 'data/raw/raw_fecomercio/cvcs',
        'estoques': 'data/raw/raw_fecomercio/iestoques',
        'icc': 'data/raw/raw_fecomercio/icc',
        'icec': 'data/raw/raw_fecomercio/icec',
        'iec': 'data/raw/raw_fecomercio/iec',
        'ipv': 'data/raw/raw_fecomercio/ipv',
        'pccv': 'data/raw/raw_fecomercio/pccv',
        'peic': 'data/raw/raw_fecomercio/peic'
    }
    for file in filenames:
        indice = file.strip().split('_')[0]
        if indice in destination:
            dest_dir = destination[indice]
            for f in os.listdir(dest_dir):
                os.remove(os.path.join(dest_dir,f))
            
            shutil.move(os.path.join(downloads_dir, file), dest_dir)
            logger.info(f"Raw file retrieved succesfuly! Saved: {dest_dir}/{file}")

def sanitize_column_names(df):
    df.columns = (
        df.columns
        .map(unidecode)
        .str.strip()
        .str.lower()
        .str.replace(r'\W+', '_', regex=True)  # substitui qualquer grupo de símbolos por 1 "_"
        .str.strip('_')  # remove "_" no começo/fim
    )
    return df


def indice_confianca_consumidor():
    """CHECA SE HA 1 ARQUIVO EXCEL, TRANSFORMA PLANILHA EM DATAFRAME E SALVA O CSV"""
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/icc/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=1, sheet_name='SÉRIE')
            df = sanitize_column_names(df)
            df = df.rename(columns={ 
                                    'icc':'icc_indice_confianca_consumidor',
                                    'icc__de_10_SM':'icc_indice_confianca_consumidor_acima_10_sm',
                                    'icea':'icea_indice_condicoes_economicas_atuais',
                                    'icea__de_10_SM':'icea_indice_condicoes_economicas_atuais_acima_10_sm'
                                    })
            df.to_csv('data/processed/fecomercio/indice_confianca_consumidor.csv',index=False, sep=';')
            logger.info("ICC dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")


def indice_endividamento_inadimplencia():
    """CHECA SE HA 1 ARQUIVO EXCEL, TRANSFORMA PLANILHA EM DATAFRAME E SALVA O CSV"""
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/peic/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica')
            df = df.iloc[:-1]
            df = df.rename(columns={'Unnamed: 0': 'mes'})
            df = df.dropna(how='all')
            df = df.dropna(axis=1,how='all')
            df = sanitize_column_names(df)
            df = df.drop(columns=['endividadas_1','contas_em_atraso_1', 'nao_terao_condicoes_de_pagar_1'])
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = df.rename(columns={
                'endividadas':'peic_indice_endividamento',
                'contas_em_atraso':'peic_indice_contas_em_atraso',
                'nao_terao_condicoes_de_pagar':'peic_indice_sem_condicoes_de_pagar'
                })
            df.to_csv('data/processed/fecomercio/indice_endividamento_inadimplencia.csv',index=False, sep=';')
            logger.info("PEIC dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")
    
def indice_confianca_empresario_comercial():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/icec/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
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
            df = sanitize_column_names(df)
            df = df.rename(columns={
                'condicoes_atuais_da_economia_cae':'cae_indice_condicoes_atuais_da_economia',
                'condicoes_atuais_das_empresas_comerciais_caec':'caec_indice_condicoes_atuais_das_empresas_comerciais',
                'condicoes_atuais_do_comercio_cac':'cac_condicoes_atuais_do_comercio',
                'expectativa_da_economia_brasileira_eeb':'eeb_expectativa_da_economia_brasileira',
                'expectativa_das_empresas_comerciais_eec':'eec_expectativa_das_empresas_comerciais',
                'expectativa_do_comercio_ec':'ec_expectativa_do_comercio',
                'indicador_de_contratacao_de_funcionarios_ic':'ic_indicador_de_contratacao_de_funcionarios',
                'nivel_de_investimento_das_empresas_nie':'nie_nivel_de_investimento_das_empresas',
                'situacao_atual_dos_estoques_sae':'sae_situacao_atual_dos_estoques',
                'indice_das_condicoes_atuais_do_empresario_do_comercio_icaec':'icaec_indice_das_condicoes_atuais_do_empresario_do_comercio',
                'indice_de_confianca_do_empresario_do_comercio_icec':'icec_indice_de_confianca_do_empresario_do_comercio',
                'indice_de_expectativa_do_empresario_do_comercio':'ieec_indice_de_expectativa_do_empresario_do_comercio',
                'indice_de_investimento_do_empresario_do_comercio':'iiec_indice_de_investimento_do_empresario_do_comercio'
            })
            df.to_csv('data/processed/fecomercio/indice_confianca_empresario_comercial.csv',index=False, sep=';')
            logger.info("ICEC dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")

def estoques():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/iestoques/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=0, sheet_name='IE - Série Histórica')
            df = df.dropna(how='all')
            df = df.dropna(how='all', axis=1)
            df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
            df = df.dropna(subset=['valor_indice'])
            df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = df.dropna(subset=['mes'])
            df = sanitize_column_names(df)
            df = df.rename(columns={
                'situacao_adequada':'indice_estoques_situacao_adequada',
                'situacao_inadequada_abaixo':'indice_estoques_situacao_inadequada_abaixo',
                'situacao_inadequada_acima':'indice_estoques_situacao_inadequada_acima',
                'indice_de_adequacao_dos_estoques':'indice_de_adequacao_dos_estoques'
            })
            df.to_csv('data/processed/fecomercio/indice_estoques.csv',index=False, sep=';')
            logger.info("iestoques dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")
        
        
        
def indice_expansao_comercio_sp():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/iec/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=0, sheet_name='IEC - Série Histórica')
            df = df.dropna(how='all')
            df = df.dropna(how='all', axis=1)
            df = df.melt(["ÍNDICES E SEGMENTAÇÕES"],var_name='mes',value_name='valor_indice')
            df = df.dropna(subset=['valor_indice'])
            df = df.pivot(index='mes', columns='ÍNDICES E SEGMENTAÇÕES', values='valor_indice').reset_index()
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = df.dropna(subset=['mes'])
            df = sanitize_column_names(df)
            df = df.rename(columns={
                'expectativas_para_contratacao_de_funcionarios':'indice_expectativas_para_contratacao_de_funcionarios',
                'nivel_de_investimento_das_empresas':'indice_nivel_de_investimento_das_empresas',
                'indice_de_expansao_do_comercio_iec':'iec_indice_de_expansao_do_comercio'
            })
            df.to_csv('data/processed/fecomercio/indice_expansao_comercio_sp.csv',index=False, sep=';')
            logger.info("ECSP dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")
        
        
def pesquisa_conjuntural_comercio_varejista():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/pccv/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica - Estado SP')
            df = df.iloc[:-4]
            df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='valor')
            df = df.rename(columns={df.columns[0]: 'comercio'})
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = df.pivot(index='mes', columns='comercio', values='valor').reset_index()
            df = sanitize_column_names(df)
            df = df.rename(columns={
                'autopecas_e_acessorios':'faturamento_autopecas_e_acessorios',
                'concessionarias_de_veiculos':'faturamento_concessionarias_de_veiculos',
                'eletrodomesticos_eletronicos_e_ld':'faturamento_eletrodomesticos_eletronicos_e_ld',
                'farmacias_e_perfumarias':'faturamento_farmacias_e_perfumarias',
                'lojas_de_moveis_e_decoracao':'faturamento_lojas_de_moveis_e_decoracao',
                'lojas_de_vestuario_tecidos_e_calcados':'faturamento_lojas_de_vestuario_tecidos_e_calcados',
                'materiais_de_construcao':'faturamento_materiais_de_construcao',
                'outras_atividades':'faturamento_outras_atividades',
                'supermercados':'faturamento_supermercados',
                'total_do_comercio_varejista':'faturamento_total_do_comercio_varejista'
            })
            df.to_csv('data/processed/fecomercio/pesquisa_conjuntural_comercio_varejista.csv',index=False, sep=';')
            logger.info("PCCV dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")

def indice_custo_vida():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/cvcs/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=0, sheet_name='Série Histórica')
            df = df.iloc[:-3]
            df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='indice_custo_de_vida')
            df = df.drop(df.columns[0],axis=1)
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = sanitize_column_names(df)
            df.to_csv('data/processed/fecomercio/custo_de_vida.csv',index=False, sep=';')
            logger.info("CVCS dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")

def indice_preco_varejo():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/ipv/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica')
            df = df.iloc[:-3]
            df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='ipv_indice_preco_varejo')
            df = df.drop(df.columns[0],axis=1)
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = sanitize_column_names(df)
            df.to_csv('data/processed/fecomercio/indice_preco_varejo.csv',index=False, sep=';')
            logger.info("IPV dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")

def indice_preco_servicos():
    folder = '/media/lucas/Files/2.Projetos/the-compass/data/raw/raw_fecomercio/ips/'
    files = os.listdir(folder)   
    if len(files) == 1 and files[0].endswith('.xlsx'):
        file_path = os.path.join(folder, files[0])
        try:
            df = pd.read_excel(file_path,header=1, sheet_name='Série Histórica')
            df = df.iloc[:-3]
            df = df.melt(id_vars=[df.columns[0]], var_name='mes', value_name='ips_indice_preco_servicos')
            df = df.drop(df.columns[0],axis=1)
            df['ips'] = pd.to_numeric(df['ips_indice_preco_servicos'], errors='coerce')
            df = df.dropna(subset=['ips'])
            df['mes'] = pd.to_datetime(df['mes'], errors='coerce').dt.date
            df = sanitize_column_names(df)
            df.to_csv('data/processed/fecomercio/indice_preco_servicos.csv',index=False,sep=';')
            logger.info("IPS dataframe converted to csv succesfuly")
        except Exception as e:
            logger.error(f"Something went wrong at conversion to csv with {file_path} --- {e}")
    else:
        logger.warning(f"Folder contains more than 1 file or it is not xlsx. Check: {folder}")