@echo off
echo 🚀 NewsDeck 시작 중...

REM .env 파일 확인
if not exist .env (
    echo ❗ .env 파일이 없습니다. 템플릿을 생성합니다.
    (
        echo # 네이버 뉴스 API 설정
        echo NAVER_CLIENT_ID=YOUR_NAVER_CLIENT_ID
        echo NAVER_CLIENT_SECRET=YOUR_NAVER_CLIENT_SECRET
        echo.
        echo # Kafka 설정
        echo KAFKA_TOPIC=news-topic
        echo.
        echo # 데이터베이스 설정
        echo DB_HOST=postgres
        echo DB_NAME=airflow
        echo DB_USER=airflow
        echo DB_PASSWORD=airflow
        echo DB_PORT=5432
        echo.
        echo # 기타 설정
        echo FETCH_INTERVAL=3600
        echo SEARCH_KEYWORDS=IT,주식,AI,반도체,경제,스타트업,블록체인
    ) > .env
    echo 📝 .env 파일에 네이버 API 키를 설정하세요!
    pause
    exit /b 1
)

REM Docker Compose 실행
echo 🐳 Docker 컨테이너 시작 중...
docker-compose up -d

echo ⏳ 서비스 초기화 대기 중...
timeout /t 30 >nul

echo ✅ NewsDeck이 성공적으로 시작되었습니다!
echo.
echo 📊 접속 정보:
echo   - Airflow: http://localhost:8080 (admin/admin)
echo   - Streamlit: http://localhost:8501
echo   - PostgreSQL: localhost:5432 (airflow/airflow)
echo.
echo 📝 상태 확인: docker-compose ps
echo 📋 로그 확인: docker-compose logs -f
pause
