import pandas as pd
from pipeline.kr_stock.transform_krx_isin import transform_krx_isin

def get_kr_stock_isin() -> pd.DataFrame:
  """KRX에서 수집한 ISIN 데이터를 사용하기 좋은 형태로 변환하여 반환하는 함수

  Returns:
    pd.DataFrame: 변환된 ISIN 데이터 (['cmp_cd', 'cmp_nm', 'isin_cd'])
  """
  return transform_krx_isin()

