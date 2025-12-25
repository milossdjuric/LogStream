cat > scripts/run-local.bat << 'EOF'
@echo off

REM Change to project root directory
cd /d "%~dp0\.."

set ROLE=%1
set ADDRESS=%2

if "%ROLE%"=="" (
    echo Usage: scripts\run-local.bat ^<leader^|follower^> [address]
    echo.
    echo Find your IP: go run cmd/netdiag/main.go
    echo.
    echo Examples:
    echo   Auto-detect IP:
    echo     scripts\run-local.bat leader
    echo     scripts\run-local.bat follower
    echo.
    echo   Specify IP:
    echo     scripts\run-local.bat leader 192.168.1.25:8001
    echo     scripts\run-local.bat follower 192.168.1.30:8002
    exit /b 1
)

if "%ROLE%"=="leader" (
    set IS_LEADER=true
) else if "%ROLE%"=="follower" (
    set IS_LEADER=false
) else (
    echo Error: role must be 'leader' or 'follower'
    exit /b 1
)

if not "%ADDRESS%"=="" (
    set NODE_ADDRESS=%ADDRESS%
)

set MULTICAST_GROUP=239.0.0.1:9999
set BROADCAST_PORT=8888

echo Starting LogStream as %ROLE%
if not "%NODE_ADDRESS%"=="" (
    echo Address: %NODE_ADDRESS%
) else (
    echo Address: auto-detect
)
echo --------------------
echo.

go run main.go
EOF