import csv
import os
from datetime import datetime

from utils.state_print import state_print

def log_error_to_csv(error_list, filename, column_list):
  """
  에러 목록을 CSV 파일로 저장하는 함수
  - 파일이 없으면 자동 생성
  - 첫 줄에 헤더 자동 작성
  - timestamp 포함하여 저장
  """
  if not error_list:
    return
  
  file_path = 'logs/errors/' + filename + '.csv'

  # 디렉토리 생성
  os.makedirs(os.path.dirname(file_path), exist_ok=True)

  # 파일이 없으면 헤더 추가
  is_new_file = not os.path.exists(file_path)

  with open(file_path, mode="a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=column_list)
    if is_new_file:
      writer.writeheader()

    for error_item in error_list:
      # Dict 형태로 변환해서 필요한 컬럼만 추려서 작성
      row = {key: error_item.get(key, "") for key in column_list}
      writer.writerow(row)
  state_print("RED", "🚨 에러 발생 : 에러 로그 생성 완료!")