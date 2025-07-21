@echo off
echo 🔍 NewsDeck 시스템 진단 시작...

echo.
echo 📊 컨테이너 상태 확인:
docker-compose ps

echo.
echo 🐘 PostgreSQL 연결 테스트:
docker-compose exec postgres psql -U airflow -d airflow -c "\dt"

echo.
echo 📰 뉴스 테이블 데이터 확인:
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) as total_news, keyword FROM news_articles GROUP BY keyword;"

echo.
echo 🔗 Kafka 토픽 확인:
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

echo.
echo 📋 Kafka 컨슈머 그룹 확인:
docker-compose exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

echo.
echo 🔍 최근 로그 확인:
echo === Producer 로그 ===
docker-compose logs --tail=10 producer

echo.
echo === Streamlit 로그 ===
docker-compose logs --tail=10 streamlit

echo.
echo === Airflow 로그 ===
docker-compose logs --tail=10 airflow-scheduler

echo.
echo ✅ 진단 완료!
pause
