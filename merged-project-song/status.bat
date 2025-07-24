@echo off
echo ====================================
echo NewsDeck 시스템 상태 체크
echo ====================================

echo [컨테이너 상태]
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo.
echo [서비스별 헬스 체크]

echo 1. Zookeeper 상태:
docker exec news_zookeeper zkServer.sh status 2>nul || echo "   - 연결 실패"

echo 2. Kafka 상태:
docker exec news_kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>nul || echo "   - 연결 실패"

echo 3. PostgreSQL 상태:
docker exec news_postgres pg_isready -U airflow 2>nul || echo "   - 연결 실패"

echo 4. Producer 로그 (최근 5줄):
docker logs news_producer --tail 5 2>nul || echo "   - 로그 없음"

echo.
echo [접속 URL]
echo Airflow: http://localhost:8080
echo Dashboard: http://localhost:8501

echo ====================================
pause
