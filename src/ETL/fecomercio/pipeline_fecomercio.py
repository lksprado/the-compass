from src.ETL.fecomercio.E_fecomercio import *
from src.ETL.fecomercio.T_fecomercio import *


def run_fecomercio_extractions():
    """BAIXA ARQUIVOS DAS URLS"""
    logger.info("Running Fecomercio extracts")
    local_version = get_local_chrome_version()
    release_version = get_latest_release_version()
    check_versions(local_version,release_version)
    urls = [
        'https://www.fecomercio.com.br/pesquisas/indice/icc',
        'https://www.fecomercio.com.br/pesquisas/indice/peic',
        'https://www.fecomercio.com.br/pesquisas/indice/icec',
        'https://www.fecomercio.com.br/pesquisas/indice/ie',
        'https://www.fecomercio.com.br/pesquisas/indice/iec',
        'https://www.fecomercio.com.br/pesquisas/indice/pccv',
        'https://www.fecomercio.com.br/pesquisas/indice/cvcs',
        'https://www.fecomercio.com.br/pesquisas/indice/ipv',
        'https://www.fecomercio.com.br/pesquisas/indice/ips',
        # 'https://www.fecomercio.com.br/pesquisas/indice/icf', ## Erro 404 // SEM SERIE HISTORICA
        # 'https://www.fecomercio.com.br/pesquisas/indice/pesp-servicos', # Erro 404
        # 'https://www.fecomercio.com.br/pesquisas/indice/pesp-comercio', # Erro 404
        # 'https://www.fecomercio.com.br/pesquisas/indice/pcss', # Erro 404
        # 'https://www.fecomercio.com.br/pesquisas/indice/imat', # Desatualizado
        # 'https://www.fecomercio.com.br/pesquisas/indice/lvc', # Desatualizado
        # 'https://www.fecomercio.com.br/pesquisas/indice/ftn' # Desatualizado
    ]
    get_indices(urls)
    print("Fecomercio extracts done!")
    print("_"*100)

def run_fecomercio_transformations():
    transformations = [
    file_mover,
    indice_confianca_consumidor,
    indice_endividamento_inadimplencia,
    indice_confianca_empresario_comercial,
    estoques,
    indice_expansao_comercio_sp,
    pesquisa_conjuntural_comercio_varejista,
    indice_custo_vida,
    indice_preco_varejo,
    indice_preco_servicos
    ]
    for transformation_func in transformations:
        try:
            print(f"Executing: {transformation_func.__name__}")
            transformation_func()
            print("_"*100)
        except Exception as e:
            logger.error(f"Something went wrong with: {transformation_func.__name__}:{e}")

def run_fecomercio_etl():
    run_fecomercio_extractions()
    run_fecomercio_transformations()



