import os
import pandas as pd
import psycopg2
from tqdm import tqdm
from pipeline.get_kr_stock_foregine import get_kr_stock_foregine
from pipeline.get_kr_stock_price import get_kr_stock_price
from utils.log_to_csv import log_error_to_csv
from utils.state_print import state_print

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

# 테이블이 없으면 자동 생성 쿼리
create_table_query = """
CREATE TABLE IF NOT EXISTS kr_stock_price ( 
  CMP_CD VARCHAR(12) NOT NULL,
  TRD_DT DATE NOT NULL,
  CLS_PRC INT NOT NULL,
  PRC_CHG INT NOT NULL,
  FLUC_RT DECIMAL(5,2),
  OPN_PRC INT NOT NULL,
  HIGH_PRC INT NOT NULL,
  LOW_PRC INT NOT NULL,
  TRD_VOL BIGINT NOT NULL,
  TRD_AMT BIGINT NOT NULL,
  MKT_CAP BIGINT NOT NULL,
  LIST_SHR BIGINT NOT NULL,

  FRG_HLD_SHR BIGINT,
  FRG_OWN_RT DECIMAL(5,2),
  FRG_LMT_SHR BIGINT,
  FRG_LMT_RT DECIMAL(5,2),

  PRIMARY KEY (CMP_CD, TRD_DT) 
);
"""

cursor.execute(create_table_query)
conn.commit()
state_print("GREEN", "✅ 테이블 확인 완료 (없으면 자동 생성)")

get_ticker_query = """
  SELECT CMP_CD, ISIN_CD, CMP_NM FROM kr_stock_ticker;
"""

cursor.execute(get_ticker_query)
tickers = cursor.fetchall()
tickers = pd.DataFrame(tickers, columns=['cmp_cd', 'isin_cd', 'cmp_nm'])

# 기존 데이터를 삭제하지 않고, 수정된 내용만 반영하는 삽입 쿼리
price_insert_query = """
  INSERT INTO kr_stock_price (
    cmp_cd, trd_dt, cls_prc, prc_chg, fluc_rt,
    opn_prc, high_prc, low_prc, trd_vol, trd_amt,
    mkt_cap, list_shr
  )
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (cmp_cd, trd_dt)
  DO UPDATE SET
    cls_prc = EXCLUDED.cls_prc,
    prc_chg = EXCLUDED.prc_chg,
    fluc_rt = EXCLUDED.fluc_rt,
    opn_prc = EXCLUDED.opn_prc,
    high_prc = EXCLUDED.high_prc,
    low_prc = EXCLUDED.low_prc,
    trd_vol = EXCLUDED.trd_vol,
    trd_amt = EXCLUDED.trd_amt,
    mkt_cap = EXCLUDED.mkt_cap,
    list_shr = EXCLUDED.list_shr;
"""

kr_stock_price_loader_error = []
# 종목별 모든 수정 주가 데이터를 데이터베이스에 저장
for _, ticker_row in tqdm(tickers.iterrows(), total=len(tickers), desc="Processing", ncols=100):
  data = get_kr_stock_price(ticker_row['cmp_cd'], ticker_row['isin_cd'], ticker_row['cmp_nm'])

  values = []
  for _, row in data.fillna(-9999).iterrows():
    record = (
      row['cmp_cd'], row['trd_dt'], row['cls_prc'],
      row['prc_chg'], row['fluc_rt'],
      row['opn_prc'], row['high_prc'], row['low_prc'],
      row['trd_vol'], row['trd_amt'],
      row['mkt_cap'], row['list_shr'],
    )
    values.append(record)

  try:
    cursor.executemany(price_insert_query, values)
    conn.commit()
  except psycopg2.errors.NumericValueOutOfRange as e:
    # 트랜잭션 롤백 (에러 발생 시 데이터베이스에 영향을 주지 않도록 함)
    kr_stock_price_loader_error.append({
    'cmp_cd': row['cmp_cd'],
    'cmp_nm': ticker_row['cmp_nm'],
    'trd_dt': row['trd_dt']
    })
    conn.rollback()  
    continue

if kr_stock_price_loader_error:
  log_error_to_csv(kr_stock_price_loader_error, 'kr_stock_price_loader_error', ['cmp_cd', 'cmp_nm', 'trd_dt'])

state_print("GREEN", "✅ 주가 데이터 삽입 및 업데이트 완료!")

foregine_update_query = """
  UPDATE kr_stock_price
  SET
    frg_hld_shr = %s,
    frg_own_rt = %s,
    frg_lmt_shr = %s,
    frg_lmt_rt = %s
  WHERE cmp_cd = %s AND trd_dt = %s;
"""

kr_stock_foregine_loader_error = []
# 종목별 모든 수정 주가 데이터를 데이터베이스에 저장
for _, ticker_row in tqdm(tickers.iterrows(), total=len(tickers), desc="Processing", ncols=100):
  data = get_kr_stock_foregine(ticker_row['cmp_cd'], ticker_row['isin_cd'], ticker_row['cmp_nm'])

  values = []
  for _, row in data.iterrows():
    record = (
      row['frg_hld_shr'],
      row['frg_own_rt'],
      row['frg_lmt_shr'],
      row['frg_lmt_rt'],
      row['cmp_cd'], row['trd_dt'],
    )
    values.append(record)
  try:
    cursor.executemany(foregine_update_query, values)
    conn.commit()
  except psycopg2.errors.NumericValueOutOfRange as e:
    # 트랜잭션 롤백 (에러 발생 시 데이터베이스에 영향을 주지 않도록 함)
    kr_stock_foregine_loader_error.append({
      'cmp_cd': row['cmp_cd'],
      'cmp_nm': tickers['cmp_nm'],
      'trd_dt': row['trd_dt']
    })
    conn.rollback()  
    continue

if kr_stock_foregine_loader_error:
  log_error_to_csv(kr_stock_foregine_loader_error, 'kr_stock_foregine_loader_error', ['cmp_cd', 'cmp_nm', 'trd_dt'])

# 연결 종료
cursor.close()
conn.close()
state_print("GREEN", "✅ 외국인 비중 데이터 삽입 및 업데이트 완료!")
