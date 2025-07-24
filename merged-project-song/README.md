# NewsDeck 통합 뉴스 수집 및 분석 시스템

## 📋 프로젝트 개요

NewsDeck은 **네이버 뉴스 API**를 활용한 실시간 뉴스 수집 및 분석 시스템입니다. song-test와 seoyunjang 프로젝트의 장점을 결합하여 만든 통합 버전입니다.

### 🏗️ 시스템 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   News Producer │───▶│     Kafka       │───▶│    Airflow      │
│ (뉴스 수집기)     │    │  (메시지 큐)      │    │ (워크플로우)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streamlit     │◀───│   PostgreSQL    │◀───│  ETL Pipeline   │
│   (대시보드)      │    │   (데이터베이스)   │    │  (데이터 처리)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 🚀 주요 기능

#### 1. 🔍 뉴스 수집기 (Producer)

- **클래스 기반 설계**: 깔끔하고 유지보수 가능한 구조
- **멀티 키워드 지원**: IT, 주식, AI, 반도체, 경제 등
- **HTML 태그 정리**: 뉴스 내용에서 불필요한 태그 제거
- **에러 처리 강화**: 안정적인 24/7 운영
- **자동 재연결**: Kafka 연결 실패 시 자동 재시도

#### 2. 📊 실시간 ETL 파이프라인 (Airflow)

- **Kafka 메시지 소비**: 실시간 뉴스 데이터 처리
- **데이터 중복 제거**: UNIQUE 제약조건으로 중복 방지
- **자동 데이터 정리**: 30일 이상 된 데이터 자동 삭제
- **통계 데이터 생성**: 키워드별, 시간대별 수집 통계
- **시스템 로그 관리**: 모든 작업 내역 기록

#### 3. 📈 대시보드 (Streamlit)

- **실시간 모니터링**: 수집 현황 실시간 확인
- **키워드별 분석**: 키워드별 수집량 및 트렌드
- **시계열 분석**: 시간대별 뉴스 수집 패턴
- **최근 뉴스 목록**: 실시간으로 수집된 최신 뉴스
- **시스템 상태 모니터링**: 에러, 경고 등 시스템 상태

#### 4. 🗄️ 데이터베이스 (PostgreSQL)

- **완전한 스키마**: 뉴스, 통계, 로그 테이블
- **성능 최적화**: 적절한 인덱스 구성
- **데이터 무결성**: UNIQUE 제약조건 및 검증

## 🛠️ 설치 및 실행

### 1. 사전 요구사항

- Docker & Docker Compose
- 네이버 개발자 API 키 ([발급받기](https://developers.naver.com/))

### 2. 환경 설정

```bash
# 프로젝트 클론
git clone https://github.com/your-repo/NewsDeck.git
cd NewsDeck/merged-project-song

# 환경 변수 설정
cp .env.example .env
# .env 파일에서 네이버 API 키 설정
```

#### .env 파일 설정

```env
# 네이버 뉴스 API 설정
NAVER_CLIENT_ID=your_naver_client_id_here
NAVER_CLIENT_SECRET=your_naver_client_secret_here

# 뉴스 수집 설정
COLLECT_KEYWORDS=IT,주식,AI,반도체,경제
COLLECT_INTERVAL=3600  # 수집 주기 (초)
ARTICLES_PER_KEYWORD=10  # 키워드당 수집할 뉴스 수
```

### 3. 시스템 실행

#### 🐳 전체 시스템 실행 (권장)

```bash
docker-compose up -d
```

#### 🎯 개별 서비스 실행

```bash
# 인프라만 실행 (Kafka, PostgreSQL, Airflow)
docker-compose up -d zookeeper kafka postgres airflow-webserver airflow-scheduler

# Producer만 추가 실행
docker-compose up -d producer

# 대시보드만 추가 실행
docker-compose up -d streamlit
```

### 4. 서비스 접속

- **Airflow 웹 UI**: http://localhost:8080 (admin/admin)
- **Streamlit 대시보드**: http://localhost:8501
- **PostgreSQL**: localhost:5432 (airflow/airflow)
- **Kafka**: localhost:9093 (외부 접속)

## 🔧 개발 환경 설정

### 로컬 개발용 Producer 실행

```bash
# Producer 폴더로 이동
cd producer

# 가상환경 생성 및 활성화
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt

# 환경 변수 설정 (로컬용)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9093

# Producer 실행
python producer.py
```

## 📁 프로젝트 구조

```
merged-project-song/
├── docker-compose.yml          # 전체 시스템 구성
├── init.sql                   # 데이터베이스 초기화
├── .env.example              # 환경 변수 예시
├── README.md                 # 이 문서
├── dags/                     # Airflow DAG
│   ├── integrated_news_etl_dag.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── __init__.py
├── producer/                 # 뉴스 수집기
│   ├── producer.py          # 메인 수집기
│   ├── Dockerfile
│   └── requirements.txt
└── streamlit/               # 대시보드
    ├── dashboard.py         # 메인 대시보드
    ├── Dockerfile
    └── requirements.txt
```

## 📊 주요 테이블 구조

### news_articles (뉴스 기사)

- `id`: 기본키
- `keyword`: 검색 키워드
- `title`: 뉴스 제목
- `link`: 뉴스 링크 (UNIQUE)
- `description`: 뉴스 내용
- `pub_date`: 발행일
- `collected_at`: 수집 시간
- `processed_at`: 처리 시간

### collection_stats (수집 통계)

- 일별/키워드별 수집 통계
- 대시보드 성능 최적화용

### system_logs (시스템 로그)

- 모든 시스템 작업 기록
- 에러 추적 및 모니터링용

## 🔍 모니터링 및 관리

### Airflow DAG 관리

1. **integrated_news_etl_pipeline**: 메인 ETL 파이프라인
   - `kafka_health_check`: Kafka 연결 확인
   - `system_health_check`: 시스템 상태 점검
   - `consume_kafka_messages`: 뉴스 데이터 처리
   - `cleanup_old_news`: 오래된 데이터 정리
   - `update_statistics`: 통계 데이터 업데이트

### 로그 확인

```bash
# 전체 로그 확인
docker-compose logs

# 특정 서비스 로그 확인
docker-compose logs producer
docker-compose logs airflow-scheduler
docker-compose logs streamlit

# 실시간 로그 확인
docker-compose logs -f producer
```

### 데이터베이스 직접 접속

```bash
docker exec -it news_postgres psql -U airflow -d airflow

# 뉴스 통계 확인
SELECT keyword, COUNT(*) FROM news_articles GROUP BY keyword;

# 최근 수집 현황
SELECT * FROM news_articles ORDER BY collected_at DESC LIMIT 10;
```

## 🚨 문제 해결

### 1. Kafka 연결 실패

- Docker 네트워크 확인: `docker network ls`
- Kafka 컨테이너 상태 확인: `docker-compose ps kafka`
- 포트 충돌 확인: `netstat -an | grep 9092`

### 2. 네이버 API 오류

- API 키 확인: `.env` 파일의 `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- API 사용량 한도 확인
- 네트워크 연결 상태 확인

### 3. 데이터베이스 연결 실패

- PostgreSQL 컨테이너 상태: `docker-compose ps postgres`
- 데이터베이스 헬스체크: `docker-compose logs postgres`

### 4. 대시보드 접속 불가

- Streamlit 컨테이너 상태: `docker-compose ps streamlit`
- 포트 8501 사용 여부 확인

## 🔄 업데이트 및 재배포

```bash
# 시스템 정지
docker-compose down

# 이미지 재빌드
docker-compose build --no-cache

# 시스템 재시작
docker-compose up -d

# 로그 확인
docker-compose logs -f
```

## 🤝 기여 방법

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📜 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 👥 개발팀

- **송채은 (song)**: 인프라 설계, Airflow ETL 파이프라인
- **장서윤 (seoyunjang)**: Producer 설계, 데이터 수집 로직
- **통합 버전**: 두 프로젝트의 장점을 결합한 완성형 시스템

---

🎉 **NewsDeck으로 실시간 뉴스 트렌드를 놓치지 마세요!**
