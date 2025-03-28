import os
from io import BytesIO
import pandas as pd
import requests as rq

from dotenv import load_dotenv
from utils import get_biz_day


def fetch_krx_foreign(cd_nm: str, isin: str, nm: str) -> pd.DataFrame:
  """KRX(한국 거래소)에서 특정 종목의 외국인 투자 관련 데이터를 반환하는 함수

  Args:
    cd_nm (str): 검색 코드 (종목 코드 / 종목 명)
    isin (str): 표준 코드(ISIN)
    nm (str): 종목명

  Returns:
    pd.DataFrame: 외국인 비중 데이터프레임 (['일자', '외국인 보유수량', '외국인 지분율', '외국인 한도수량', '외국인 한도소진율'])

  Raises:
    Exception: KRX 서버에서 데이터 접근이 거부될 경우 예외 발생
  """
  # 최신 영업일
  biz_day = get_biz_day()

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
  'locale': 'ko_KR',
  'searchType': '2',
  'mktId': 'ALL',
  'tboxisuCd_finder_stkisu0_7': cd_nm,
  'isuCd': isin,
  'codeNmisuCd_finder_stkisu0_7': nm,
  'param1isuCd_finder_stkisu0_7': 'ALL',
  'strtDd': '19000101',
  'endDd': {biz_day},
  'share': '1',
  'csvxls_isNo': 'false',
  'name': 'fileDown',
  'url': 'dbms/MDC/STAT/standard/MDCSTAT03702'
  }

  # KRX에 OTP 발급 요청 및 데이터 다운로드
  otp_code = rq.post(gen_otp_url, data=gen_otp_params, headers=headers).text.strip()
  response = rq.post(down_url, data={'code': otp_code}, headers=headers)
        
  if "Access Denied" in response.text:
    raise Exception(f"❌ 시장 데이터 접근이 거부되었습니다. 헤더와 OTP 요청을 확인하세요.")
        
  data =  pd.read_csv(BytesIO(response.content), encoding='EUC-KR')
  return data[['일자', '외국인 보유수량', '외국인 지분율', '외국인 한도수량', '외국인 한도소진율']]