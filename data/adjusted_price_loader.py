import os
import psycopg2
from tqdm import tqdm
from pipeline.get_kr_stock_isin import get_kr_stock_isin
from pipeline.get_kr_adjusted_price import get_kr_adjusted_price
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
  CMP_CD VARCHAR(10) NOT NULL,    
  TRADE_DATE DATE NOT NULL,       
  CLOSE_PRICE INT NOT NULL,       
  PRICE_DIFF INT NOT NULL,        
  FLUCTUATION_RT DECIMAL(5,2),    
  OPEN_PRICE INT NOT NULL,        
  HIGH_PRICE INT NOT NULL,        
  LOW_PRICE INT NOT NULL,         
  TRADE_VOLUME BIGINT NOT NULL,   
  TRADE_VALUE BIGINT NOT NULL,    
  MARKET_CAP BIGINT NOT NULL,     
  LISTED_SHARES BIGINT NOT NULL,  
  PRIMARY KEY (CMP_CD, TRADE_DATE)
);

"""
cursor.execute(create_table_query)
conn.commit()
state_print("GREEN", "✅ 테이블 확인 완료 (없으면 자동 생성)")

# 기존 데이터를 삭제하지 않고, 수정된 내용만 반영하는 삽입 쿼리
insert_query = """
INSERT INTO kr_stock_price (
  CMP_CD, TRADE_DATE, CLOSE_PRICE, PRICE_DIFF, FLUCTUATION_RT, 
  OPEN_PRICE, HIGH_PRICE, LOW_PRICE, TRADE_VOLUME, 
  TRADE_VALUE, MARKET_CAP, LISTED_SHARES
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (CMP_CD, TRADE_DATE) DO UPDATE 
SET 
  CLOSE_PRICE = EXCLUDED.CLOSE_PRICE,
  PRICE_DIFF = EXCLUDED.PRICE_DIFF,
  FLUCTUATION_RT = EXCLUDED.FLUCTUATION_RT,
  OPEN_PRICE = EXCLUDED.OPEN_PRICE,
  HIGH_PRICE = EXCLUDED.HIGH_PRICE,
  LOW_PRICE = EXCLUDED.LOW_PRICE,
  TRADE_VOLUME = EXCLUDED.TRADE_VOLUME,
  TRADE_VALUE = EXCLUDED.TRADE_VALUE,
  MARKET_CAP = EXCLUDED.MARKET_CAP,
  LISTED_SHARES = EXCLUDED.LISTED_SHARES;
"""

# 종목 리스트 (ISIN 데이터 포함)
isin_data = get_kr_stock_isin()

# 종목별 모든 수정 주가 데이터를 데이터베이스에 저장
for _, row in tqdm(isin_data.iterrows(), total=len(isin_data), desc="Processing", ncols=100):
  data = get_kr_adjusted_price(row['cmp_cd'], row['isin_cd'], row['cmp_nm'])

  values = []
  for _, price_row in data.iterrows():
    record = (
      row['cmp_cd'], price_row['trade_date'], price_row['close_price'],
      price_row['price_diff'], price_row['fluctuation_rt'],
      price_row['open_price'], price_row['high_price'], price_row['low_price'],
      price_row['trade_volume'], price_row['trade_value'],
      price_row['market_cap'], price_row['listed_shares']
    )
    values.append(record)

  try:
    cursor.executemany(insert_query, values)
    conn.commit()
  except psycopg2.errors.NumericValueOutOfRange as e:
    print("\n🚨 **NumericValueOutOfRange 발생!** 🚨")
    print(f"오류 발생 종목: {row['cmp_cd']} ({row['cmp_nm']})")
    print("🔍 삽입 시도 데이터:")
        
    # 컬럼명 정의
    columns = [
      "CMP_CD", "TRADE_DATE", "CLOSE_PRICE", "PRICE_DIFF", "FLUCTUATION_RT",
      "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "TRADE_VOLUME",
      "TRADE_VALUE", "MARKET_CAP", "LISTED_SHARES"
    ]

    # 오류 발생한 데이터 출력
    for idx, record in enumerate(values):
      print(f"[{idx+1}] ", end="")
      for col_name, value in zip(columns, record):
        if isinstance(value, int) and abs(value) > 2147483647:  # INT 범위 초과 값 강조 표시
          print("RED", f"🚨 {col_name}: {value} (⚠ 범위 초과!)", end=" | ")
        else:
          print(f"{col_name}: {value}", end=" | ")
      print()
        
    conn.rollback()  # 트랜잭션 롤백 (에러 발생 시 데이터베이스에 영향을 주지 않도록 함)
    continue  # 다음 종목 처리


# 연결 종료
cursor.close()
conn.close()
state_print("GREEN", "✅ 데이터 삽입 및 업데이트 완료!")
