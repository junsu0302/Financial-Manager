from io import BytesIO, StringIO
import os
import requests as rq
from dotenv import load_dotenv
import pandas as pd

def fetch_isin() -> pd.DataFrame:
  """KRX(한국 거래소)에서 한국 주식 종목별 표준코드(ISIN)를 반환하는 함수

  Returns:
    pd.DataFrame: ['cmp_cd' (종목 코드), 'isin_cd' (표준코드)] (단축코드 = 종목 코드)

  Raises:
    Exception: KRX 서버에서 데이터 접근이 거부될 경우 예외 발생
  """
  # 환경 변수 로드
  load_dotenv()
  gen_otp_url = os.getenv('KRX_GEN_OTP_URL')
  down_url = os.getenv('KRX_DOWN_URL')
  headers = {
    'Referer': os.getenv('KRX_REFERER'),
    'User-Agent': os.getenv('USER_AGENT')
  }

  # KRX API 요청 파라미터
  gen_otp_params = {
    "locale": 'ko_KR',
    'mktId': 'ALL',
    'share': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
  }

  # KRX에 OTP 발급 요청 및 데이터 다운로드
  otp_code = rq.post(gen_otp_url, data=gen_otp_params, headers=headers).text.strip()
  response = rq.post(down_url, data={'code': otp_code}, headers=headers)
      
  if "Access Denied" in response.text:
    raise Exception(f"❌ 시장 데이터 접근이 거부되었습니다. 헤더와 OTP 요청을 확인하세요.")

  # 데이터 변환
  data =  pd.read_csv(BytesIO(response.content), encoding='EUC-KR')
  data = data.rename(columns={'단축코드': 'cmp_cd', '표준코드': 'isin_cd'})

  return data.loc[:, ['cmp_cd', 'isin_cd']]