import pandas as pd
from pipeline.kr_stock.transform_krx_adjusted_price import transform_krx_adjusted_price
from pipeline.kr_stock.transform_krx_foreign import transform_krx_foreign


def get_kr_stock_price(cd: str, isin: str, nm: str) -> pd.DataFrame:
  """KRX에서 수집한 특정 종목의 수정 주가 데이터를 사용하기 좋은 형태로 변환하여 반환하는 함수

  Args:
    cd (str): 종목 코드
    isin (str): 표준 코드 (ISIN)
    nm (str): 종목 명

  Returns:
    pd.DataFrame: 변환된 수정 주가 데이터 (['cmp_cd', 'trade_date', 'close_price', 'price_diff', 'fluctuation_rt', 'open_price', 'high_price', 'low_price', 'trade_volume', 'trade_value', 'market_cap', 'listed_shares'])
  """
  kr_adjusted_price = transform_krx_adjusted_price(cd, isin, nm)

  return kr_adjusted_price