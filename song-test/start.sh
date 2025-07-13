#!/bin/bash

echo "🚀 NewsDeck 시작 중..."

# .env 파일 확인
if [ ! -f .env ]; then
    echo "❗ .env 파일이 없습니다. 템플릿을 생성합니다."
    cat > .env << EOF
# 네이버 뉴스 API 설정
NAVER_CLIENT_ID=YOUR_NAVER_CLIENT_ID
NAVER_CLIENT_SECRET=YOUR_NAVER_CLIENT_SECRET

# Kafka 설정
KAFKA_TOPIC=news-topic

# 데이터베이스 설정
DB_HOST=postgres
DB_NAME=airflow
DB_USER=airflow
DB_PASSWORD=airflow
DB_PORT=5432

# 기타 설정
FETCH_INTERVAL=3600
SEARCH_KEYWORDS=IT,주식,AI,반도체,경제,스타트업,블록체인
EOF
    echo "📝 .env 파일에 네이버 API 키를 설정하세요!"
    exit 1
fi

# Docker Compose 실행
echo "🐳 Docker 컨테이너 시작 중..."
docker-compose up -d

echo "⏳ 서비스 초기화 대기 중..."
sleep 30

echo "✅ NewsDeck이 성공적으로 시작되었습니다!"
echo ""
echo "📊 접속 정보:"
echo "  - Airflow: http://localhost:8080 (admin/admin)"
echo "  - Streamlit: http://localhost:8501"
echo "  - PostgreSQL: localhost:5432 (airflow/airflow)"
echo ""
echo "📝 상태 확인: docker-compose ps"
echo "📋 로그 확인: docker-compose logs -f"
