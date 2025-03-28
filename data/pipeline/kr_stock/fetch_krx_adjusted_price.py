import os
from io import BytesIO
import pandas as pd
import requests as rq

from dotenv import load_dotenv
from utils import get_biz_day


def fetch_krx_adjusted_price(cd_nm: str, isin: str, nm: str) -> pd.DataFrame:
  """KRX(한국 거래소)에서 특정 종목의 수정 주가 데이터를 반환하는 함수
  수정주가 :  주식의 액면분할, 무상증자, 유상증자, 배당 등과 같은 기업의 자본 변동이나 시장 이벤트가 발생했을 때, 과거 주가를 해당 이벤트에 맞춰 보정한 가격

  Args:
    cd_nm (str): 검색 코드 (종목 코드 / 종목 명)
    isin (str): 표준 코드(ISIN)
    nm (str): 종목명

  Returns:
    pd.DataFrame: 수정 주가 데이터 (['일자', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'])

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
  # - `strtDd`를 20000101 설정하여 가능한 한 가장 오래된 데이터를 요청
  # - `endDd`는 최신 영업일(`biz_day`)로 설정하여 최신 데이터까지 포함
  gen_otp_params = {
  'locale': 'ko_KR',    
  'tboxisuCd_finder_stkisu0_1': cd_nm,
  'isuCd': isin,
  'codeNmisuCd_finder_stkisu0_1': nm,
  'param1isuCd_finder_stkisu0_1': 'ALL',
  'strtDd': 19000101,
  'endDd': biz_day,
  'adjStkPrc_check': 'Y',
  'adjStkPrc': '1',
  'share': '1',
  'money': '1',
  'csvxls_isNo': 'false',
  'name': 'fileDown',
  'url': 'dbms/MDC/STAT/standard/MDCSTAT01701'
  }

  # KRX에 OTP 발급 요청 및 데이터 다운로드
  otp_code = rq.post(gen_otp_url, data=gen_otp_params, headers=headers).text.strip()
  response = rq.post(down_url, data={'code': otp_code}, headers=headers)
        
  if "Access Denied" in response.text:
    raise Exception(f"❌ 시장 데이터 접근이 거부되었습니다. 헤더와 OTP 요청을 확인하세요.")
        
  data =  pd.read_csv(BytesIO(response.content), encoding='EUC-KR')
  return data