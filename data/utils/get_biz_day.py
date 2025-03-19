from datetime import date, timedelta

def get_biz_day():
  """현재 시점에서 가장 최근 영업일을 반환한다. 영업일은 주말과 공휴일을 제외한 업무 처리가 가능한 날을 의미한다.

  Returns:
      str: 'YYYYMMDD' 형식으로 반환
  """
  today = date.today()
  if today.weekday() == 5:
    today -= timedelta(days=1)
  elif today.weekday() == 6:
    today -= timedelta(days=2)

  return today.strftime('%Y%m%d')