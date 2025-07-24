@echo off
echo ====================================
echo NewsDeck 통합 시스템 안전 재시작
echo ====================================

echo 1. 모든 컨테이너 중지...
docker-compose down

echo 2. Docker 네트워크 정리...
docker network prune -f

echo 3. 사용하지 않는 볼륨 정리...
docker volume prune -f

echo 4. 시스템 재시작 (순서대로)...
docker-compose up -d zookeeper
timeout /t 10

docker-compose up -d kafka
timeout /t 15

docker-compose up -d postgres
timeout /t 10

docker-compose up -d airflow-init
timeout /t 20

docker-compose up -d airflow-webserver airflow-scheduler
timeout /t 10

docker-compose up -d producer streamlit

echo 5. 컨테이너 상태 확인...
docker ps

echo ====================================
echo 재시작 완료! 브라우저에서 확인하세요.
echo Airflow: http://localhost:8080 (admin/admin)
echo Dashboard: http://localhost:8501
echo ====================================
pause
