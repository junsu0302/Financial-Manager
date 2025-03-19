from .kr_stock.transform_krx_sector import transform_krx_sector
from utils.state_print import state_print


def get_kr_stock_sector():
  """한국 주식 섹터 데이터 반환"""
  kr_stock_sector = transform_krx_sector()
  return kr_stock_sector