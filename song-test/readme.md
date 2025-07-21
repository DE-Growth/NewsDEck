## Kafka는 Docker로, 수집기는 로컬에서 실행하기

- 설정 방법
인프라 실행: 터미널에서 docker-compose up -d kafka zookeeper postgres airflow-webserver airflow-scheduler 명령으로 producer를 제외한 핵심 서비스만 실행합니다.

- 로컬 개발 환경 설정:

PC의 producer 폴더에 venv 같은 파이썬 가상 환경을 만듭니다.

가상 환경을 활성화하고 pip install -r requirements.txt로 라이브러리를 로컬에 직접 설치합니다.

- Kafka 접속 주소 변경:

로컬에서 실행하는 producer.py가 Docker 안의 Kafka로 접속하려면, 접속 주소가 kafka:9092가 아닌 localhost:9093이어야 합니다. (docker-compose.yml에서 외부 접속용으로 설정한 포트입니다.)

producer.py의 Kafka 접속 부분을 다음과 같이 수정하거나, 실행 시 환경 변수를 설정해 줍니다.

## Python
```
# producer.py
#로컬에서 실행할 경우 'localhost:9093'을 기본값으로 사용
KAFKA_BROKER = os.getenv("KAFKA_BROKER", 'localhost:9093')

```
## 개발 워크플로우
docker-compose up -d ...로 인프라를 실행해 둡니다.
VS Code 같은 IDE에서 producer 폴더를 열고, producer.py 파일을 직접 실행(F5 또는 python producer.py)하며 개발합니다.

코드를 수정하면 IDE에서 바로 재실행하며 결과를 확인합니다. IDE의 디버거, 중단점(breakpoint) 등 모든 기능을 활용할 수 있습니다.

장점: 가장 빠르고 직관적입니다. 평소 파이썬 스크립트를 개발하는 것과 동일한 경험을 제공하며, IDE의 강력한 디버깅 기능을 100% 활용할 수 있습니다.