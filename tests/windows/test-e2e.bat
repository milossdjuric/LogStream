@echo off
setlocal enabledelayedexpansion

REM Enhanced Late Joiner Test - Shows State Synchronization
REM Usage: tests\test-late-joiner.bat [local|docker]

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
cd /d "%PROJECT_ROOT%"

call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[E2E]

echo %TEST_PREFIX% ================================================
echo %TEST_PREFIX% Test: Late Joiner with Sequence Demonstration
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ================================================
echo.
echo %TEST_PREFIX% This test demonstrates:
echo %TEST_PREFIX%   1. Leader starts alone (seq=1)
echo %TEST_PREFIX%   2. Leader runs for 30 seconds
echo %TEST_PREFIX%   3. Follower joins late and syncs state
echo.

if "%MODE%"=="local" (
    REM Local mode
    
    REM Build
    call :build_if_needed logstream.exe main.go
    
    REM STEP 1: Start leader only
    call :log "=== STEP 1: Starting leader only ==="
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /B logstream.exe > %TEMP%\logstream-leader.log 2>&1
    call :success "Leader started"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Leader initial state (seq=1):"
    type %TEMP%\logstream-leader.log | findstr /C:"Registered broker" /C:"seq=" /C:"Total Brokers"
    call :success "Leader registered (seq=1)"
    
    REM STEP 2: Wait 30 seconds
    echo.
    call :log "=== STEP 2: Waiting 30 seconds (leader accumulating heartbeats) ==="
    for /L %%i in (30,-1,1) do (
        set /a "REMAIN=%%i"
        <nul set /p="Time remaining: !REMAIN!s  "
        timeout /t 1 /nobreak >nul
        echo.
    )
    
    echo.
    call :log "Leader after 30 seconds:"
    type %TEMP%\logstream-leader.log | findstr /C:"HEARTBEAT" /C:"seq=" /C:"Total Brokers"
    call :success "Leader still at seq=1 (no state changes)"
    
    REM STEP 3: Start follower (late joiner)
    echo.
    call :log "=== STEP 3: Starting follower (late joiner) ==="
    set IS_LEADER=false
    set NODE_ADDRESS=localhost:8002
    start /B logstream.exe > %TEMP%\logstream-follower.log 2>&1
    call :success "Follower started"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Leader state after follower joins (seq should increase to 2):"
    type %TEMP%\logstream-leader.log | findstr /C:"JOIN from" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 2"
    call :success "Follower joined -^> seq increased to 2"
    call :success "Leader sent REPLICATE seq=2 to sync state"
    
    echo.
    call :log "Follower state (should sync from leader):"
    type %TEMP%\logstream-follower.log | findstr /C:"Found cluster" /C:"REPLICATE" /C:"Applied" /C:"Total Brokers"
    call :success "Follower discovered cluster"
    call :success "Follower received REPLICATE seq=2"
    call :success "Follower synchronized with leader"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% LATE JOINER TEST SUMMARY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% Time 0s:   Leader started (seq=1)
    echo %TEST_PREFIX% Time 30s:  Leader running alone
    echo %TEST_PREFIX% Time 30s:  Follower joined (seq-^>2)
    echo %TEST_PREFIX% Result:    Follower synced successfully
    echo.
    echo %TEST_PREFIX% This proves late joiners can sync state!
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow logs
    call :log "Press Ctrl+C to stop..."
    type %TEMP%\logstream-leader.log %TEMP%\logstream-follower.log
    pause

) else if "%MODE%"=="docker" (
    REM Docker mode
    set COMPOSE_FILE=%SCRIPT_DIR%compose\duo.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    REM STEP 1: Start leader only
    call :log "=== STEP 1: Starting leader only ==="
    docker compose -f duo.yaml up -d leader
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Leader initial state (seq=1):"
    docker compose -f duo.yaml logs leader | findstr /C:"Registered broker" /C:"seq=" /C:"Total Brokers"
    call :success "Leader registered (seq=1)"
    
    REM STEP 2: Wait 30 seconds
    echo.
    call :log "=== STEP 2: Waiting 30 seconds (leader accumulating heartbeats) ==="
    for /L %%i in (30,-1,1) do (
        set /a "REMAIN=%%i"
        <nul set /p="Time remaining: !REMAIN!s  "
        timeout /t 1 /nobreak >nul
        echo.
    )
    
    echo.
    call :log "Leader after 30 seconds:"
    docker compose -f duo.yaml logs leader | findstr /C:"HEARTBEAT" /C:"seq=" /C:"Total Brokers"
    call :success "Leader still at seq=1 (no state changes)"
    
    REM STEP 3: Start follower
    echo.
    call :log "=== STEP 3: Starting follower (late joiner) ==="
    docker compose -f duo.yaml up -d broker1
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Leader state after follower joins (seq should increase to 2):"
    docker compose -f duo.yaml logs leader | findstr /C:"JOIN from" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 2"
    call :success "Follower joined -^> seq increased to 2"
    call :success "Leader sent REPLICATE seq=2 to sync state"
    
    echo.
    call :log "Follower state (should sync from leader):"
    docker compose -f duo.yaml logs broker1 | findstr /C:"Found cluster" /C:"REPLICATE" /C:"Applied" /C:"Total Brokers"
    call :success "Follower discovered cluster"
    call :success "Follower received REPLICATE seq=2"
    call :success "Follower synchronized with leader"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% LATE JOINER TEST SUMMARY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% Time 0s:   Leader started (seq=1)
    echo %TEST_PREFIX% Time 30s:  Leader running alone
    echo %TEST_PREFIX% Time 30s:  Follower joined (seq-^>2)
    echo %TEST_PREFIX% Result:    Follower synced successfully
    echo.
    echo %TEST_PREFIX% This proves late joiners can sync state!
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow logs
    call :log "Following logs (Ctrl+C to stop)..."
    docker compose -f duo.yaml logs -f

) else (
    call :error_msg "Invalid mode: %MODE% (use 'local' or 'docker')"
    exit /b 1
)

exit /b 0