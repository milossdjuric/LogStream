@echo off
setlocal enabledelayedexpansion

REM Enhanced Trio Test - Shows Sequence 1 -> 2 -> 3
REM Usage: tests\test-trio.bat [local|docker|vagrant]

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
cd /d "%PROJECT_ROOT%"

call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[TRIO]

echo %TEST_PREFIX% ================================================
echo %TEST_PREFIX% Test: Sequential Broker Joins (seq=1-^>2-^>3)
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ================================================
echo.

if "%MODE%"=="local" (
    REM Local mode
    
    REM Build
    call :build_if_needed logstream.exe main.go
    
    REM STEP 1: Leader only (seq=1)
    call :log "STEP 1: Starting leader (seq will be 1)..."
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /B logstream.exe > %TEMP%\logstream-leader.log 2>&1
    call :success "Leader started"
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Current state - Leader only:"
    type %TEMP%\logstream-leader.log | findstr /C:"Registered broker" /C:"seq=" | more +0
    call :success "seq=1: Leader registered itself"
    
    timeout /t 3 /nobreak >nul
    
    REM STEP 2: Add follower 1 (seq=2)
    echo.
    call :log "STEP 2: Adding follower 1 (seq will increase to 2)..."
    set IS_LEADER=false
    set NODE_ADDRESS=localhost:8002
    start /B logstream.exe > %TEMP%\logstream-follower1.log 2>&1
    call :success "Follower 1 started"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader + Follower 1:"
    type %TEMP%\logstream-leader.log | findstr /C:"JOIN from" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 2"
    call :success "seq=2: Follower 1 joined, REPLICATE seq=2 sent"
    
    timeout /t 3 /nobreak >nul
    
    REM STEP 3: Add follower 2 (seq=3)
    echo.
    call :log "STEP 3: Adding follower 2 (seq will increase to 3)..."
    set NODE_ADDRESS=localhost:8003
    start /B logstream.exe > %TEMP%\logstream-follower2.log 2>&1
    call :success "Follower 2 started"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader + 2 Followers:"
    type %TEMP%\logstream-leader.log | findstr /C:"JOIN from" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 3"
    call :success "seq=3: Follower 2 joined, REPLICATE seq=3 sent"
    
    REM Show final state on all nodes
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% FINAL STATE - ALL NODES:
    echo %TEST_PREFIX% =========================================
    echo.
    echo %TEST_PREFIX% Leader registry:
    type %TEMP%\logstream-leader.log | findstr /C:"=== Registry Status ==="
    echo.
    echo %TEST_PREFIX% Follower 1 registry:
    type %TEMP%\logstream-follower1.log | findstr /C:"=== Registry Status ==="
    echo.
    echo %TEST_PREFIX% Follower 2 registry:
    type %TEMP%\logstream-follower2.log | findstr /C:"=== Registry Status ==="
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% SEQUENCE PROGRESSION:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower 1 joined
    echo %TEST_PREFIX% seq=3: Follower 2 joined
    echo.
    echo %TEST_PREFIX% All nodes synchronized at seq=3
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Show heartbeats
    call :log "Current heartbeat exchange (seq=0 for all):"
    timeout /t 3 /nobreak >nul
    echo.
    echo %TEST_PREFIX% Last 3 heartbeats from each node:
    echo.
    echo %TEST_PREFIX% Leader:
    type %TEMP%\logstream-leader.log | findstr /C:"-> HEARTBEAT"
    echo.
    echo %TEST_PREFIX% Follower 1:
    type %TEMP%\logstream-follower1.log | findstr /C:"-> HEARTBEAT"
    echo.
    echo %TEST_PREFIX% Follower 2:
    type %TEMP%\logstream-follower2.log | findstr /C:"-> HEARTBEAT"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% KEY TAKEAWAY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% REPLICATE messages: seq=1,2,3... (state changes)
    echo %TEST_PREFIX% HEARTBEAT messages: seq=0 (periodic pings)
    echo.
    echo %TEST_PREFIX% Sequences only increase when state changes!
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow all logs
    call :log "Press Ctrl+C to stop..."
    type %TEMP%\logstream-leader.log %TEMP%\logstream-follower1.log %TEMP%\logstream-follower2.log
    pause

) else if "%MODE%"=="docker" (
    REM Docker mode
    set COMPOSE_FILE=%SCRIPT_DIR%compose\trio.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    REM STEP 1: Leader only
    call :log "STEP 1: Starting leader (seq will be 1)..."
    docker compose -f trio.yaml up -d leader
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Current state - Leader only:"
    docker compose -f trio.yaml logs leader | findstr /C:"Registered broker" /C:"seq="
    call :success "seq=1: Leader registered itself"
    
    timeout /t 3 /nobreak >nul
    
    REM STEP 2: Add follower 1
    echo.
    call :log "STEP 2: Adding follower 1 (seq will increase to 2)..."
    docker compose -f trio.yaml up -d broker1
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader + Follower 1:"
    docker compose -f trio.yaml logs leader | findstr /C:"JOIN" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 2"
    call :success "seq=2: Follower 1 joined, REPLICATE seq=2 sent"
    
    timeout /t 3 /nobreak >nul
    
    REM STEP 3: Add follower 2
    echo.
    call :log "STEP 3: Adding follower 2 (seq will increase to 3)..."
    docker compose -f trio.yaml up -d broker2
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader + 2 Followers:"
    docker compose -f trio.yaml logs leader | findstr /C:"JOIN" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 3"
    call :success "seq=3: Follower 2 joined, REPLICATE seq=3 sent"
    
    REM Show final state
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% FINAL STATE - ALL NODES:
    echo %TEST_PREFIX% =========================================
    echo.
    echo %TEST_PREFIX% Leader registry:
    docker compose -f trio.yaml logs leader | findstr /C:"=== Registry Status ==="
    echo.
    echo %TEST_PREFIX% Follower 1 registry:
    docker compose -f trio.yaml logs broker1 | findstr /C:"=== Registry Status ==="
    echo.
    echo %TEST_PREFIX% Follower 2 registry:
    docker compose -f trio.yaml logs broker2 | findstr /C:"=== Registry Status ==="
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% SEQUENCE PROGRESSION:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower 1 joined
    echo %TEST_PREFIX% seq=3: Follower 2 joined
    echo.
    echo %TEST_PREFIX% All nodes synchronized at seq=3
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Show heartbeats
    call :log "Current heartbeat exchange (seq=0 for all):"
    timeout /t 3 /nobreak >nul
    echo.
    echo %TEST_PREFIX% Last 3 heartbeats from each node:
    echo.
    echo %TEST_PREFIX% Leader:
    docker compose -f trio.yaml logs leader | findstr /C:"-> HEARTBEAT"
    echo.
    echo %TEST_PREFIX% Follower 1:
    docker compose -f trio.yaml logs broker1 | findstr /C:"-> HEARTBEAT"
    echo.
    echo %TEST_PREFIX% Follower 2:
    docker compose -f trio.yaml logs broker2 | findstr /C:"-> HEARTBEAT"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% KEY TAKEAWAY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% REPLICATE messages: seq=1,2,3... (state changes)
    echo %TEST_PREFIX% HEARTBEAT messages: seq=0 (periodic pings)
    echo.
    echo %TEST_PREFIX% Sequences only increase when state changes!
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow all logs
    call :log "Following all logs (Ctrl+C to stop)..."
    docker compose -f trio.yaml logs -f

) else if "%MODE%"=="vagrant" (
    cd /d "%PROJECT_ROOT%\deploy\vagrant"
    
    call :log "Checking Vagrant VMs..."
    for %%v in (leader broker1 broker2) do (
        vagrant status %%v 2>nul | findstr /C:"running" >nul
        if errorlevel 1 (
            call :error_msg "%%v VM not running! Run: vagrant up"
            exit /b 1
        )
    )
    
    call :log "STEP 1: Starting leader (seq will be 1)..."
    vagrant ssh leader -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.10:8001 IS_LEADER=true MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream.log 2>&1 &"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader only:"
    vagrant ssh leader -c "tail -20 /tmp/logstream.log 2>/dev/null" 2>nul | findstr /C:"Registered broker" /C:"seq="
    call :success "seq=1: Leader registered itself"
    
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "STEP 2: Adding follower 1 (seq will increase to 2)..."
    vagrant ssh broker1 -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.20:8002 IS_LEADER=false MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream.log 2>&1 &"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader + Follower 1:"
    vagrant ssh leader -c "tail -40 /tmp/logstream.log 2>/dev/null" 2>nul | findstr /C:"JOIN" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 2"
    call :success "seq=2: Follower 1 joined, REPLICATE seq=2 sent"
    
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "STEP 3: Adding follower 2 (seq will increase to 3)..."
    vagrant ssh broker2 -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.30:8003 IS_LEADER=false MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream.log 2>&1 &"
    timeout /t 5 /nobreak >nul
    
    echo.
    call :log "Current state - Leader + 2 Followers:"
    vagrant ssh leader -c "tail -40 /tmp/logstream.log 2>/dev/null" 2>nul | findstr /C:"JOIN" /C:"Registered broker" /C:"REPLICATE seq=" /C:"Total Brokers: 3"
    call :success "seq=3: Follower 2 joined, REPLICATE seq=3 sent"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% SEQUENCE PROGRESSION:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower 1 joined
    echo %TEST_PREFIX% seq=3: Follower 2 joined
    echo.
    echo %TEST_PREFIX% All nodes synchronized at seq=3
    echo %TEST_PREFIX% =========================================
    echo.
    call :success "Trio test complete (Vagrant)"
    
    REM Cleanup
    vagrant ssh leader -c "pkill -f logstream" 2>nul
    vagrant ssh broker1 -c "pkill -f logstream" 2>nul
    vagrant ssh broker2 -c "pkill -f logstream" 2>nul

) else (
    call :error_msg "Invalid mode: %MODE% (use 'local', 'docker', or 'vagrant')"
    exit /b 1
)

exit /b 0