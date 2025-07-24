FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /usr/src/app

# 시스템 패키지 업데이트 및 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY . .

# 환경 변수 설정
ENV PYTHONPATH=/usr/src/app
ENV PYTHONUNBUFFERED=1

# 실행 명령
CMD ["python", "producer.py"]
