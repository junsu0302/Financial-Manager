import os

import pandas as pd
import psycopg2

from pipeline.kr_stock.fetch_krx_foreign import fetch_krx_foreign

def transform_krx_foreign(cd: str, isin: str, nm: str) -> pd.DataFrame:
  """KRX(한국 거래소)에서 수집한 원본 수정 주가 데이터를 변환하는 함수

  Args:
    cd (str): 종목 코드
    isin (str): 표준 코드 (ISIN)
    nm (str): 종목 명

  Returns:
    pd.DataFrame: 변환된 수정 주가 데이터 (['trd_dt', 'frg_hld_shr', 'frg_own_rt', 'frg_lmt_shr', 'frg_lmt_rt'])
  """

  # KRX에서 원본 수정 주가 데이터 수집
  cd_nm = cd + '/' + nm
  foreign_df = fetch_krx_foreign(cd_nm, isin, nm)

  # 컬럼명 변경 매핑 (한글 컬럼명 → 영문 컬럼명)
  column_mapping = {
    '일자': 'trd_dt',
    '외국인 보유수량': 'frg_hld_shr',
    '외국인 지분율': 'frg_own_rt',
    '외국인 한도수량': 'frg_lmt_shr',
    '외국인 한도소진율': 'frg_lmt_rt'
  }
  foreign_df = foreign_df.rename(columns=column_mapping)

  # 최신 거래 데이터가 위에 오도록 trade date를 기준으로 정렬 
  foreign_df = foreign_df.sort_values(by='trd_dt', ascending=False)

  foreign_df.insert(0, 'cmp_cd', cd)

  return foreign_df[['cmp_cd', 'trd_dt', 'frg_hld_shr', 'frg_own_rt', 'frg_lmt_shr', 'frg_lmt_rt']]