@echo off
echo ====================================
echo NewsDeck 시스템 재시작 옵션 선택
echo ====================================
echo.
echo 1. 빠른 재시작 (stop/start) - 데이터 보존
echo 2. 완전 재시작 (down/up) - 네트워크 재생성  
echo 3. 초기화 재시작 (down -v) - 모든 데이터 삭제
echo.
set /p choice="선택하세요 (1-3): "

if "%choice%"=="1" goto quick_restart
if "%choice%"=="2" goto full_restart  
if "%choice%"=="3" goto clean_restart
echo 잘못된 선택입니다.
pause
exit

:quick_restart
echo ====================================
echo 빠른 재시작 실행중...
echo ====================================
docker-compose stop
timeout /t 5
docker-compose start
goto check_status

:full_restart
echo ====================================
echo 완전 재시작 실행중...
echo ====================================
docker-compose down
docker network prune -f
timeout /t 5
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
goto check_status

:clean_restart
echo ====================================
echo ⚠️  경고: 모든 데이터가 삭제됩니다!
echo ====================================
set /p confirm="정말 진행하시겠습니까? (y/N): "
if not "%confirm%"=="y" if not "%confirm%"=="Y" (
    echo 취소되었습니다.
    pause
    exit
)
docker-compose down -v
docker system prune -f
docker volume prune -f
timeout /t 5
docker-compose up -d zookeeper
timeout /t 10
docker-compose up -d kafka
timeout /t 15
docker-compose up -d postgres
timeout /t 15
docker-compose up -d airflow-init
timeout /t 25
docker-compose up -d airflow-webserver airflow-scheduler
timeout /t 15
docker-compose up -d producer streamlit
goto check_status

:check_status
echo ====================================
echo 컨테이너 상태 확인
echo ====================================
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.
echo 재시작 완료! 브라우저에서 확인하세요.
echo Airflow: http://localhost:8080 (admin/admin)
echo Dashboard: http://localhost:8501
echo ====================================
pause
