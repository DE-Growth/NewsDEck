FROM apache/airflow:2.8.1

# 필요한 Python 패키지 설치
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 사용자 권한 설정
USER airflow
