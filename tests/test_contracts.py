import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd 
import pandera as pa 
import numpy as np
import pytest

from src.contracts import Railway

### TESTAR SE O CONTRATO DE DADOS ESTÁ CERTO

def test_contrato_correto():
    df_test = pd.DataFrame(
        {
            "mes_ano":['2022-01-01', '2024-02-01'],
            "ferrovia":['ABC','EDM'],
            "mercadoria_antt":['SOJA','FERRO'],
            "estacao_origem":['ARACAJU','CAMPO MOURAO'],
            "uf_origem":['MA','SP'],
            "uf_destino":['SP','RJ'],
            "estacao_destino":['SANTOS','PORTO'],
            "tu":[10000,3000],
            "tku":[300000,500000],
            "estimated_distance_km":[357.32, 867.2],
        }
    )
    Railway.validate(df_test)

def test_valores_negativos():
    df_test = pd.DataFrame(
        {
            "mes_ano":['2022-01-01', '2024-02-01'],
            "ferrovia":['ABC','EDM'],
            "mercadoria_antt":['SOJA','FERRO'],
            "estacao_origem":['ARACAJU','CAMPO MOURAO'],
            "uf_origem":['MA','SP'],
            "uf_destino":['SP','RJ'],
            "estacao_destino":['SANTOS','PORTO'],
            "tu":[-10000,3000],
            "tku":[300000,-500000],
            "estimated_distance_km":[357.32, 867.2],
        }
    )
    with pytest.raises(pa.errors.SchemaError):
        Railway.validate(df_test)