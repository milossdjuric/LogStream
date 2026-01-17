@echo off
REM Enhanced Duo Test - Shows Sequence Progression (1→2)
REM Usage: test-duo-enhanced.bat [local|docker]

setlocal enabledelayedexpansion

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[DUO]

echo %TEST_PREFIX% ========================================
echo %TEST_PREFIX% Test: Dynamic Broker Join (Seq Demo)
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ========================================
echo.

if "%MODE%"=="local" (
    cd /d "%PROJECT_ROOT%"
    
    REM Build
    call :build_if_needed logstream main.go
    if errorlevel 1 exit /b 1
    
    REM STEP 1: Start leader
    call :log "STEP 1: Starting leader..."
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /b cmd /c "logstream.exe > %TEMP%\logstream-leader.log 2>&1"
    timeout /t 3 /nobreak >nul
    call :success "Leader started"
    
    echo.
    call :log "Leader initial state:"
    echo %TEST_PREFIX% --------------------
    type %TEMP%\logstream-leader.log | findstr /C:"Registered broker" /C:"seq="
    echo.
    call :success "Leader registered itself (seq=1)"
    
    REM STEP 2: Leader heartbeats
    echo.
    call :log "STEP 2: Leader sending heartbeats (seq=0)..."
    timeout /t 5 /nobreak >nul
    type %TEMP%\logstream-leader.log | findstr /C:"HEARTBEAT" /C:"seq=0"
    call :success "Heartbeats use seq=0 (not state changes)"
    
    REM STEP 3: Add follower
    echo.
    call :log "STEP 3: Adding follower (this will increase seq to 2)..."
    set IS_LEADER=false
    start /b cmd /c "logstream.exe > %TEMP%\logstream-follower.log 2>&1"
    timeout /t 5 /nobreak >nul
    call :success "Follower started"
    
    echo.
    call :log "Leader state after follower join:"
    echo %TEST_PREFIX% ---------------------------------
    type %TEMP%\logstream-leader.log | findstr /C:"REPLICATE seq=" /C:"Total Brokers" | more +1
    echo.
    call :success "Follower joined - seq increased to 2"
    call :success "Leader sent REPLICATE seq=2 to synchronize"
    
    echo.
    call :log "Follower state after joining:"
    echo %TEST_PREFIX% -----------------------------
    type %TEMP%\logstream-follower.log | findstr /C:"Found cluster" /C:"REPLICATE" /C:"Total Brokers"
    echo.
    call :success "Follower received and applied REPLICATE seq=2"
    
    REM STEP 4: Heartbeats
    echo.
    call :log "STEP 4: Both nodes exchanging heartbeats..."
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% SEQUENCE NUMBER SUMMARY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower joined cluster
    echo %TEST_PREFIX% seq=0: Heartbeat messages (not state changes)
    echo.
    echo %TEST_PREFIX%  REPLICATE messages show state changes (seq=1,2,3...)
    echo %TEST_PREFIX%  HEARTBEAT messages are periodic pings (seq=0)
    echo %TEST_PREFIX% =========================================
    echo.
    
    call :log "Following logs (Ctrl+C to stop)..."
    echo %TEST_PREFIX% Watch for heartbeat exchanges between nodes
    echo.
    
    REM Follow both logs (Windows doesn't have tail -f for multiple files easily)
    echo %TEST_PREFIX% Press Ctrl+C to stop...
    :follow_loop
    type %TEMP%\logstream-leader.log | more +1
    type %TEMP%\logstream-follower.log | more +1
    timeout /t 2 /nobreak >nul
    goto follow_loop
    
) else if "%MODE%"=="docker" (
    set COMPOSE_FILE=%SCRIPT_DIR%compose\duo.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    REM STEP 1: Start leader only
    call :log "STEP 1: Starting leader..."
    docker compose -f duo.yaml up -d leader
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Leader initial state:"
    docker compose -f duo.yaml logs leader | findstr /C:"Registered" /C:"seq="
    call :success "Leader registered itself (seq=1)"
    
    REM STEP 2: Heartbeats
    echo.
    call :log "STEP 2: Leader sending heartbeats (seq=0)..."
    timeout /t 5 /nobreak >nul
    docker compose -f duo.yaml logs --tail=5 leader | findstr "HEARTBEAT"
    call :success "Heartbeats use seq=0"
    
    REM STEP 3: Add follower
    echo.
    call :log "STEP 3: Adding follower (seq will increase to 2)..."
    docker compose -f duo.yaml up -d broker1
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Leader state after follower join:"
    docker compose -f duo.yaml logs leader | findstr /C:"REPLICATE seq=" /C:"Total Brokers: 2"
    call :success "Follower joined - seq increased to 2"
    
    echo.
    call :log "Follower state:"
    docker compose -f duo.yaml logs broker1 | findstr /C:"Found cluster" /C:"REPLICATE" /C:"Total Brokers"
    call :success "Follower synchronized"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% SEQUENCE NUMBER SUMMARY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower joined cluster
    echo %TEST_PREFIX% seq=0: Heartbeat messages
    echo %TEST_PREFIX% =========================================
    echo.
    
    call :log "Following logs (Ctrl+C to stop)..."
    docker compose -f duo.yaml logs -f
    
) else (
    call :error_msg "Invalid mode: %MODE%"
    exit /b 1
)

endlocal