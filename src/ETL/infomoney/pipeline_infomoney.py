from src.ETL.infomoney.E_infomoney import *
from src.ETL.infomoney.T_infomoney import *
from utils.logger import get_logger
logger = get_logger(__name__)

def run_infomoney_extractions():
    nonce = get_nonce_with_selenium()
    if nonce:
        get_json_data(nonce)
        return True
    else:
        logger.warning("Failed to get nonce element")
        return False

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
    
def run_infomoney_pipeline():
    logger.info("Initiating Future Interests pipeline from Infomoney...")
    extraction = run_infomoney_extractions()
    if extraction:
        run_infomoney_transformations()
    else:
        logger.warning("⚠️  Pipeline execution stopped due failure on extraction")
    logger.info("✅ Future Interests Infomoney pipeline completed!")
    logger.info("-"*100)