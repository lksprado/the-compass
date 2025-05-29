import pandas as pd 
from src.contracts import EnergyConsumoNumconsSamUf,EnergySetorIndustrialUf
from pandera.errors import SchemaError, SchemaErrors
from utils.logger import get_logger
logger = get_logger(__name__)


def make_fuel_df(file_path, output_path):
    df = pd.read_excel(file_path, header=None)
    df.columns = df.iloc[16]
    df = df.iloc[17:].reset_index(drop=True)
    df.to_csv(f"{output_path}/fuels_prices.csv", index=False, sep=';')
    logger.info(f"File saved successfully at: {output_path}/fuels_prices.csv")
    return df 


def make_energy_df(sheet, output_name, file_path, output_path):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet)
        if sheet == 'CONSUMO E NUMCONS SAM UF':
            df = EnergyConsumoNumconsSamUf.validate(df, lazy=True) 
            df.columns = [col.lower() for col in df.columns]
            df['data'] = pd.to_datetime(df['data'], format='%Y%m%d')
            df = df.drop(columns=['dataexcel', 'dataversao'])
            object_cols = df.select_dtypes(include='object').columns
            df[object_cols] = df[object_cols].apply(lambda x: x.str.lower())
            df.rename(columns={'data':'mes_ano','consumo':'consumo_mwh'},inplace=True)
            df.to_csv(f"{output_path}/{output_name}.csv", index=False, sep=';')
            logger.info(f"File saved successfully at {output_path}/{output_name}.csv")
        elif sheet == 'SETOR INDUSTRIAL POR UF':
            df = EnergySetorIndustrialUf.validate(df, lazy=True) 
            df.columns = [col.lower() for col in df.columns]
            df['data'] = pd.to_datetime(df['data'], format='%Y%m%d')
            df = df.drop(columns=['dataexcel', 'dataversao'])
            object_cols = df.select_dtypes(include='object').columns
            df[object_cols] = df[object_cols].apply(lambda x: x.str.lower())
            df.rename(columns={'data':'mes_ano','consumo':'consumo_mwh'},inplace=True)
            df.to_csv(f"{output_path}/{output_name}.csv", index=False, sep=';')
            logger.info(f"File saved successfully at: {output_path}/{output_name}.csv")

    except (SchemaError, SchemaErrors) as e:
        logger.error(f"❗ Validation Error in dataframe schema: {e}")
        logger.error(f"Details: {e.failure_cases}")
        return None

    except Exception as e:
        logger.error(f"❗ TRANSFORMATION failed: {e}")
        return None