cat > scripts/stop-docker.bat << 'EOF'
@echo off

REM Change to project root directory
cd /d "%~dp0\.."

echo Stopping LogStream Docker containers...
echo --------------------------------------
echo.

docker compose down

echo.
echo ================================
echo All containers stopped and removed
echo Network removed
echo ================================
echo.
echo To restart: scripts\run-docker.bat
EOF