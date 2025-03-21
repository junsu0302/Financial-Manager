import os
import psycopg2
import pandas as pd
from pipeline.get_kr_stock_ticker import get_kr_stock_ticker
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
  CREATE TABLE IF NOT EXISTS kr_stock_ticker (
    CMP_CD VARCHAR(10) PRIMARY KEY,  -- 종목 코드
    ISIN_CD VARCHAR(20) NOT NULL,    -- 표준 종목 코드 (ISIN)
    CMP_NM VARCHAR(255) NOT NULL,    -- 종목명
    MKT_TYPE VARCHAR(10) NOT NULL,   -- 시장 구분 (KOSPI/KOSDAQ)
    GICS_CD VARCHAR(10),             -- GICS 코드
    MKT_CAP_RT DECIMAL(10,6),        -- GICS 코드 내 종목의 시가총액 비율
    REF_DT DATE NOT NULL             -- 기준일
  );
"""
cursor.execute(create_table_query)
conn.commit()
state_print("GREEN", "✅ 테이블 확인 완료 (없으면 자동 생성)")

# 기존 데이터를 삭제하지 않고, 수정된 내용만 반영하는 삽입 쿼리
insert_query = """
  INSERT INTO kr_stock_ticker (CMP_CD, ISIN_CD, CMP_NM, MKT_TYPE, GICS_CD, MKT_CAP_RT, REF_DT)
  VALUES (%s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (CMP_CD) DO UPDATE 
  SET ISIN_CD = EXCLUDED.ISIN_CD,
    CMP_NM = EXCLUDED.CMP_NM,
    MKT_TYPE = EXCLUDED.MKT_TYPE,
    GICS_CD = EXCLUDED.GICS_CD,
    MKT_CAP_RT = EXCLUDED.MKT_CAP_RT,
    REF_DT = EXCLUDED.REF_DT;
"""

kr_stock_ticker = get_kr_stock_ticker()

# 데이터 삽입 및 업데이트
for _, row in kr_stock_ticker.iterrows():
  cursor.execute(insert_query, tuple(row))

# 변경사항 반영 및 연결 종료
conn.commit()
cursor.close()
conn.close()
state_print("GREEN", "✅ 데이터 삽입 및 업데이트 완료!")
