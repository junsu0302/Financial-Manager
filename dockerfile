# 베이스 이미지
FROM python:3.12

# 시스템 패키지 업데이트 & Poetry 설치
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# 작업 디렉토리 설정
WORKDIR /app

# Poetry 설정: 가상환경을 Docker 컨테이너에 직접 설치되게
ENV POETRY_VIRTUALENVS_CREATE=false

# Poetry 설정 파일 복사
COPY pyproject.toml poetry.lock ./

# 디펜던시 설치
RUN poetry install --no-root

# 코드 복사
COPY ./app ./app

# 기본 실행 명령
CMD ["python", "app/your_code.py"]
