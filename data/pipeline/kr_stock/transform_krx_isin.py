from io import BytesIO, StringIO
import os
import psycopg2
import pandas as pd
from datetime import datetime

from pipeline.kr_stock.fetch_krx_isin import fetch_krx_isin

def transform_krx_isin() -> pd.DataFrame:
  """KRX(한국 거래소)에서 수집한 INIS 데이터를 변환하는 함수. DB에 보유한 종목 데이터와 병합 수행

  Returns:
    pd.DataFrame: 변환된 ISIN 데이터 (['cmp_cd', 'cmp_nm', 'isin_cd'])
  """

  # KRX에서 ISIN 데이터 수집
  isin_df = fetch_krx_isin()

  # PostgreSQL 연결 정보
  DB_PARAMS = {
    "dbname": os.getenv("STOCK_DB_NAME"),
    "user": os.getenv("POSTGRESQL_USER"),
    "password": os.getenv("POSTGRESQL_PASSWORD"),
    "host": os.getenv("POSTGRESQL_HOST"),
    "port": os.getenv("POSTGRESQL_PORT")
  }

  # PostgreSQL 연결
  conn = psycopg2.connect(**DB_PARAMS)
  cursor = conn.cursor()

  # 보유한 종목 리스트 조회
  query = "SELECT cmp_cd, cmp_nm FROM kr_stock_sector;"
  cursor.execute(query)

  # 데이터를 DataFrame으로 변환
  columns = [desc[0] for desc in cursor.description]
  df = pd.DataFrame(cursor.fetchall(), columns=columns)

  # 연결 종료
  cursor.close()
  conn.close()

  merged_df = df.merge(isin_df, on='cmp_cd', how='left')

  return merged_df
