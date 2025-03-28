import os

import pandas as pd
import psycopg2

from pipeline.kr_stock.fetch_krx_adjusted_price import fetch_krx_adjusted_price

def transform_krx_adjusted_price(cd: str, isin: str, nm: str) -> pd.DataFrame:
  """KRX(한국 거래소)에서 수집한 원본 수정 주가 데이터를 변환하는 함수

  Args:
    cd (str): 종목 코드
    isin (str): 표준 코드 (ISIN)
    nm (str): 종목 명

  Returns:
    pd.DataFrame: 변환된 수정 주가 데이터 (['cmp_cd', 'trade_date', 'close_price', 'price_diff', 'fluctuation_rt', 'open_price', 'high_price', 'low_price', 'trade_volume', 'trade_value', 'market_cap', 'listed_shares'])
  """

  # KRX에서 원본 수정 주가 데이터 수집
  cd_nm = cd + '/' + nm
  adjusted_price_df = fetch_krx_adjusted_price(cd_nm, isin, nm)

  # 컬럼명 변경 매핑 (한글 컬럼명 → 영문 컬럼명)
  column_mapping = {
    '일자': 'trd_dt',
    '종가': 'cls_prc',
    '대비': 'prc_chg',
    '등락률': 'fluc_rt',
    '시가': 'opn_prc',
    '고가': 'high_prc',
    '저가': 'low_prc',
    '거래량': 'trd_vol',
    '거래대금': 'trd_amt',
    '시가총액': 'mkt_cap',
    '상장주식수': 'list_shr'
  }
  adjusted_price_df = adjusted_price_df.rename(columns=column_mapping)

  # 모든 수정 주가 데이터에 종목 코드 추가(Key)
  adjusted_price_df['cmp_cd'] = cd
  adjusted_price_df = adjusted_price_df[['cmp_cd'] + [col for col in adjusted_price_df.columns if col != "cmp_cd"]]

  # 최신 거래 데이터가 위에 오도록 trade date를 기준으로 정렬 
  adjusted_price_df = adjusted_price_df.sort_values(by='trd_dt', ascending=False)

  return adjusted_price_df
