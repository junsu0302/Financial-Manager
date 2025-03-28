from pipeline.kr_stock.transform_krx_ticker import transform_krx_ticker
from pipeline.kr_stock.fetch_krx_isin import fetch_krx_isin


def get_kr_stock_ticker():
  """한국 주식 섹터 데이터 반환"""
  kr_stock_ticker = transform_krx_ticker()
  kr_stock_isin = fetch_krx_isin()
  
  kr_stock_ticker = kr_stock_ticker.merge(kr_stock_isin, on='cmp_cd', how='left')
  
  return kr_stock_ticker[['cmp_cd', 'isin_cd', 'cmp_nm', 'mkt_type', 'gics_cd', 'mkt_cap_rt', 'ref_dt']]