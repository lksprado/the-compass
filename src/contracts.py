import pandera as pa
from pandera.typing import Series
from pandera import DataFrameModel
from typing import Optional

## coerce força a conversão dos tipos de dados do dataframe

VALID_UFS = {
        'RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE',
        'AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF'
    }

class Railway(DataFrameModel):
    mes_ano: Series[pa.DateTime]
    ferrovia: Series[str]
    mercadoria_antt: Series[str]
    mercadoria_en: Series[str]
    estacao_origem: Series[str]
    uf_origem: Series[str]
    uf_destino: Series[str]
    tu: Series[int] = pa.Field(gt=0)
    tku: Series[int] = pa.Field(gt=0)
    estimated_distance_km: Series[int] = pa.Field(gt=0)
    file: Series[str]

    class Config:
        strict = False
        coerce = True
        drop_invalid_rows=True

    @pa.check("uf_origem", name="Checagem código UF na origem")
    def check_uf_origem(cls, s: Series[str]):
        return s.isin(VALID_UFS)

    @pa.check("uf_destino", name="Checagem código UF no destino")
    def check_uf_destino(cls, s: Series[str]):
        return s.isin(VALID_UFS)


class Toll(DataFrameModel):
    mes_ano: Series[pa.DateTime]
    concessionaria: Series[str]
    praca: Series[str]
    tipo_cobranca: Optional[Series[str]] = pa.Field(nullable=True)
    tipo_de_veiculo: Series[str]
    volume_total: Series[int] = pa.Field(gt=0)

    class Config:
        strict = False
        coerce = True
        drop_invalid_rows=True

class EnergyConsumoNumconsSamUf(DataFrameModel):
    Data: Series[int]	
    DataExcel: Series[pa.Date]
    UF: Series[str]
    Regiao: Series[str]
    Sistema: Series[str]
    Classe: Series[str]
    TipoConsumidor: Series[str]
    Consumo: Series[float]
    Consumidores: Series[int]
    DataVersao: Series[pa.Date]
    class Config:
        strict = False
        coerce = True
        drop_invalid_rows=True
        
    @pa.check("UF", name="Checagem código UF")
    def check_uf(cls, s: Series[str]):
        return s.isin(VALID_UFS)

class EnergySetorIndustrialUf(DataFrameModel):
    Data: Series[int]	
    DataExcel: Series[pa.Date]
    SetorIndustrial: Series[str]
    UF: Series[str]
    Regiao: Series[str]    
    Consumo: Series[float]
    DataVersao: Series[pa.Date]
    class Config:
        strict = False
        coerce = True
        drop_invalid_rows=True
        
    @pa.check("UF", name="Checagem código UF")
    def check_uf(cls, s: Series[str]):
        return s.isin(VALID_UFS)