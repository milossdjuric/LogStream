@echo off
REM Single Leader Test - Enhanced
REM Usage: test-single-enhanced.bat [local|docker]

setlocal enabledelayedexpansion

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

REM Source common functions
call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[SINGLE]

echo %TEST_PREFIX% ========================================
echo %TEST_PREFIX% Test: Single Leader with Sequence Demo
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ========================================
echo.

if "%MODE%"=="local" (
    cd /d "%PROJECT_ROOT%"
    
    REM Build
    call :build_if_needed logstream main.go
    if errorlevel 1 exit /b 1
    
    REM Start leader
    call :log "Starting leader..."
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /b cmd /c "logstream.exe > %TEMP%\logstream-leader.log 2>&1"
    timeout /t 3 /nobreak >nul
    call :success "Leader started"
    
    echo.
    call :log "Initial state - Leader only (seq=1)"
    echo %TEST_PREFIX% -----------------------------------
    type %TEMP%\logstream-leader.log | findstr /C:"Registered broker" /C:"seq="
    
    echo.
    call :log "Heartbeat phase (seq=0 is correct for heartbeats)"
    echo %TEST_PREFIX% --------------------------------------------------
    timeout /t 5 /nobreak >nul
    type %TEMP%\logstream-leader.log | findstr /C:"HEARTBEAT" /C:"seq=" | more +1
    
    echo.
    call :log "Notice: HEARTBEAT messages use seq=0 (they're not state changes)"
    call :log "REPLICATE messages use incrementing seq (they ARE state changes)"
    
    echo.
    call :log "Following logs (Ctrl+C to stop)..."
    echo %TEST_PREFIX% Look for:
    echo %TEST_PREFIX%   - HEARTBEAT messages (seq=0) - Periodic pings
    echo %TEST_PREFIX%   - REPLICATE messages (seq=1+) - State synchronization
    echo.
    type %TEMP%\logstream-leader.log
    
    REM Wait for Ctrl+C
    pause
    
) else if "%MODE%"=="docker" (
    set COMPOSE_FILE=%SCRIPT_DIR%compose\single.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    call :log "Starting leader..."
    docker compose -f single.yaml up -d
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Initial state - Leader only (seq=1)"
    echo %TEST_PREFIX% -----------------------------------
    docker compose -f single.yaml logs | findstr /C:"seq=" /C:"Registered" /C:"REPLICATE"
    
    echo.
    call :log "Heartbeat phase (seq=0 is correct for heartbeats)"
    echo %TEST_PREFIX% --------------------------------------------------
    timeout /t 5 /nobreak >nul
    docker compose -f single.yaml logs --tail=10 | findstr /C:"HEARTBEAT" /C:"seq="
    
    echo.
    call :log "Notice: HEARTBEAT messages use seq=0 (they're not state changes)"
    call :log "REPLICATE messages use incrementing seq (they ARE state changes)"
    
    echo.
    call :log "Following logs (Ctrl+C to stop)..."
    docker compose -f single.yaml logs -f
    
) else (
    call :error_msg "Invalid mode: %MODE% (use 'local' or 'docker')"
    exit /b 1
)

endlocal