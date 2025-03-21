import os
import pandas as pd
import requests as rq

from io import BytesIO
from dotenv import load_dotenv

from data.utils.get_biz_day import get_biz_day
from data.utils.state_print import state_print

# 영업일 가져오기
biz_day = get_biz_day()

# OTP 코드 생성 (KRX에서 데이터를 다운로드하기 위해 필요)
load_dotenv()
gen_otp_url = os.getenv('KRX_GEN_OTP_URL')
down_url = os.getenv('KRX_DOWN_URL')
headers = {
  'Referer': os.getenv('KRX_REFERER'),
  'User-Agent': os.getenv('USER_AGENT')
}

def krx_ticker_loader(market_id:str) -> pd.DataFrame:
  """
  KRX에서 특정 시장(KOSPI/KOSDAQ)의 업종 데이터를 가져오는 함수
    
  Args:
    market_id (str): 시장 ID ('STK' - KOSPI, 'KSQ' - KOSDAQ)
    
  Returns:
    pd.DataFrame: 해당 시장의 업종 데이터가 포함된 Pandas DataFrame
    
  Raises:
    Exception: KRX 서버에서 데이터 접근이 거부될 경우 예외 발생
  """
  gen_otp_params = {
    'mktId': market_id,
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
  }
    
  otp_code = rq.post(gen_otp_url, data=gen_otp_params, headers=headers).text.strip()
  response = rq.post(down_url, data={'code': otp_code}, headers=headers)
    
  if "Access Denied" in response.text:
    raise Exception(f"❌ {market_id} 시장 데이터 접근이 거부되었습니다. 헤더와 OTP 요청을 확인하세요.")
    
  return pd.read_csv(BytesIO(response.content), encoding='EUC-KR')

def fetch_krx_ticker():
  """KRX에서 가져온 KOSPI/KOSDAQ 데이터를 병합하여 반환하는 함수

  Returns:
    pd.DataFrame: ['종목코드', '종목명', '시장구분', '업종명', '종가', '대비', '등락률', '시가총액', '기준일']
  """
  # KOSPI & KOSDAQ 업종 데이터 가져오기
  try:
    sector_stk = krx_ticker_loader('STK')
    state_print("WHITE", "- KOSPI 업종 데이터 다운로드 완료")
    sector_ksq = krx_ticker_loader('KSQ')
    state_print("WHITE", "- KOSDAQ 업종 데이터 다운로드 완료")
  except Exception as e:
    state_print("RED", str(e))
    exit()

  # KOSPI & KOSDAQ 데이터 병합
  krx_ticker = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)

  # 데이터 클리닝
  krx_ticker['종목명'] = krx_ticker['종목명'].str.strip()  # 종목명 공백 제거
  krx_ticker['기준일'] = biz_day  # 기준일 컬럼 추가

  # '기준일' 컬럼을 날짜 형식(YYYY-MM-DD)으로 변환
  krx_ticker['기준일'] = pd.to_datetime(krx_ticker['기준일'], format='%Y%m%d', errors='coerce')

  state_print("GREEN", "✅ KOSPI/KOSDAQ 업종 데이터 수집 완료")

  return krx_ticker