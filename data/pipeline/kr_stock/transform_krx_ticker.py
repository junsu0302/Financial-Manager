import pandas as pd
from .fetch_krx_ticker import fetch_krx_ticker
from utils.state_print import state_print

def add_gics_column(krx_ticker:pd.DataFrame) -> pd.DataFrame:
  """KRX 업종명을 바탕으로 GICS 코드 생성

  Args:
    krx_ticker (pd.DataFrame): GICS 코드를 추가할 한국 주식 섹터 정보 

  Returns:
    pd.DataFrame: [cmp_cd, cmp_nm, mkt_type, gics_cd, mkt_cap_rt, ref_dt]
  """
  # KRX 업종명을 GICS 기준으로 매핑
  gics_mapping = {
    "비금속": "15", "금속": "15", "종이": "15", "화학": "15",
    "기계": "20", "일반서비스": "20", "건설": "20", "운송": "20", "기타제조": "20",
    "섬유": "25", "운송장비": "25", "유통": "25",
    "농업": "30", "음식료": "30",
    "제약": "35", "의료": "35",
    "금융": "40", "은행": "40", "증권": "40", "보험": "40", "기타금융": "40",
    "IT 서비스": "45", "전자": "45",
    "출판":"50", "오락": "50", "통신": "50",
    "전기": "55", "가스": "55", "수도": "55",
    "부동산": "60"
  }

  # GICS 매핑 함수
  def get_gics_info(sec_name:str) -> str:
    """GICS 정보를 적절하게 매핑

    Args:
      sec_name (str): 현재 입력되어 있는 업종명

    Returns:
      str: 업종명에 따른 GICS 코드
    """
    for keyword, gics_code in gics_mapping.items():
      if keyword in sec_name:
        return gics_code
    return "N/A"

  # KRX 섹터 데이터에 GICS 컬럼 추가 (업종명 기반 매핑)
  krx_ticker[["gics_code"]] = krx_ticker["업종명"].apply(lambda x: pd.Series(get_gics_info(str(x))))

  state_print("WHITE", "- GICS Code 생성 완료")

  return krx_ticker

def add_gics_rate_column(krx_ticker:pd.DataFrame) -> pd.DataFrame:
  """GICS 섹터에서 현재 주식의 총 비율을 반환하는 함수

  Args:
    krx_ticker (pd.DataFrame): GICS 섹터가 존재하는 주식 데이터

  Returns:
    pd.DataFrame: GICS 비율이 포함된 주식 데이터
  """
  # gics_code별 시가총액 총합 계산
  krx_ticker['시가총액'] = pd.to_numeric(krx_ticker['시가총액'], errors='coerce')
  total_market_cap_by_gics = krx_ticker.groupby('gics_code')['시가총액'].sum().reset_index()
  total_market_cap_by_gics = total_market_cap_by_gics.rename(columns={'시가총액': 'gics_total_market_cap'})

  # 원래 데이터프레임과 병합
  krx_ticker = krx_ticker.merge(total_market_cap_by_gics, on='gics_code', how='left')

  # 각 종목의 gics_code 내 시가총액 비율 계산
  krx_ticker['시가총액_비율'] = (krx_ticker['시가총액'] / krx_ticker['gics_total_market_cap']) * 100

  state_print("WHITE", "- GICS Code Rate 생성 완료")

  return krx_ticker

def rename_columns(krx_ticker:pd.DataFrame) -> pd.DataFrame:
  """한국 주식의 이름을 변경

  Args:
    krx_ticker (pd.DataFrame): 이름을 변경할 한국 주식 데이터

  Returns:
    pd.DataFrame: 이름이 변경된 한국 주식 데이터
  """
  krx_ticker = krx_ticker[['종목코드', '종목명', '시장구분', 'gics_code', '시가총액_비율', '기준일']]
  krx_ticker = krx_ticker.rename(columns={
    '종목코드': 'cmp_cd', #기업 코드
    '종목명': 'cmp_nm', # 기업 이름
    '시장구분': 'mkt_type', # 시장 구분
    'gics_code': 'gics_cd', # GICS(산업분류) 코드
    '시가총액_비율': 'mkt_cap_rt',  # GICS 내 시가총액 비율
    '기준일': 'ref_dt'  # 데이터 기준일 
  })

  state_print("WHITE", "- 주식 데이터 컬럼 명 변경 완료")

  return krx_ticker

def transform_krx_ticker():
  """KOSPI/KOSDAQ 업종 데이터 변환"""
  krx_ticker = fetch_krx_ticker()
  krx_ticker = add_gics_column(krx_ticker)
  krx_ticker = add_gics_rate_column(krx_ticker)
  krx_ticker = rename_columns(krx_ticker)
  state_print("GREEN", "✅ KOSPI/KOSDAQ 업종 데이터 변환 완료")
  return krx_ticker
