cat > scripts/run-docker.bat << 'EOF'
@echo off

REM Change to project root directory
cd /d "%~dp0\.."

echo LogStream Docker Test
echo --------------------
echo.

echo Building and starting containers...
docker compose up --build -d

echo.
echo Waiting for startup...
timeout /t 5 /nobreak >nul

echo.
echo Container status:
docker compose ps

echo.
echo Leader logs:
docker compose logs --tail=15 leader

echo.
echo Broker1 logs:
docker compose logs --tail=15 broker1

echo.
echo Broker2 logs:
docker compose logs --tail=15 broker2

echo.
echo --------------------
echo Containers running. Use 'docker compose logs -f' to watch logs.
echo Stop with 'scripts\stop-docker.bat' or 'docker compose down'
EOF