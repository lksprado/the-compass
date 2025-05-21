from src.ETL.infomoney.E_infomoney import *
from src.ETL.infomoney.T_infomoney import *


def run_infomoney_extractions():
    logger.info("Running Infomoney extracts")
    nonce = get_nonce_with_selenium()
    if nonce:
        get_json_data(nonce)
    else:
        logger.warning("Failed to get nonce element")
        logger.warning("-"*50)

def run_infomoney_transformations():
    input = 'data/raw/raw_interest_rates'
    output = 'data/processed/interest_rates'
    files_list = os.listdir(input)
    df_list = []
    for file in files_list:
        file_name = os.path.join(input,file)
        df = parse_interest(file_name)
        df_list.append(df)
    
    final_df = pd.concat(df_list, ignore_index=False)
    final_df.to_csv(f'{output}/interest_rate.csv',sep=';',index=False)
    
def run_infomoney_etl():
    run_infomoney_extractions()
    run_infomoney_transformations()