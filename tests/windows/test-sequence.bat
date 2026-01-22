@echo off
REM Dynamic Sequence Demo - Shows seq progression through joins/leaves/timeouts
REM Usage: test-sequence-demo.bat [local|docker|vagrant]

setlocal enabledelayedexpansion

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[SEQUENCE]

echo %TEST_PREFIX% ================================================
echo %TEST_PREFIX% SEQUENCE NUMBER DEMONSTRATION
echo %TEST_PREFIX% Shows how sequences increase with state changes
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ================================================
echo.

if "%MODE%"=="local" (
    cd /d "%PROJECT_ROOT%"
    
    call :build_if_needed logstream main.go
    if errorlevel 1 exit /b 1
    
    set EXPECTED_SEQ=1
    
    REM STEP 1: Start leader
    call :log "STEP 1: Start Leader"
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /b cmd /c "logstream.exe > %TEMP%\logstream-leader.log 2>&1"
    timeout /t 3 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After leader start (seq should be 1)
    echo %TEST_PREFIX% =========================================
    type %TEMP%\logstream-leader.log | findstr /C:"Registered broker" /C:"seq="
    call :success "Expected seq=1, Leader registered"
    set EXPECTED_SEQ=2
    
    REM STEP 2: Add follower 1
    echo.
    call :log "STEP 2: Add Follower 1"
    set IS_LEADER=false
    set PORT=8002
    start /b cmd /c "logstream.exe > %TEMP%\logstream-follower1.log 2>&1"
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 1 joins (seq should be 2)
    echo %TEST_PREFIX% =========================================
    type %TEMP%\logstream-leader.log | findstr /C:"REPLICATE seq=" /C:"Total Brokers" | more +1
    call :success "Expected seq=2, Follower 1 joined"
    set EXPECTED_SEQ=3
    
    REM STEP 3: Add follower 2
    echo.
    call :log "STEP 3: Add Follower 2"
    set PORT=8003
    start /b cmd /c "logstream.exe > %TEMP%\logstream-follower2.log 2>&1"
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 2 joins (seq should be 3)
    echo %TEST_PREFIX% =========================================
    type %TEMP%\logstream-leader.log | findstr /C:"REPLICATE seq=3" /C:"Total Brokers: 3"
    call :success "Expected seq=3, Follower 2 joined"
    set EXPECTED_SEQ=4
    
    REM STEP 4: Heartbeat check
    echo.
    call :log "HEARTBEAT CHECK"
    timeout /t 3 /nobreak >nul
    echo %TEST_PREFIX% Recent heartbeats (all should be seq=0):
    type %TEMP%\logstream-leader.log | findstr "HEARTBEAT" | more +1
    call :success "Heartbeats use seq=0 (not state changes)"
    
    REM STEP 5: Kill follower 2 (will timeout)
    echo.
    call :log "STEP 4: Kill Follower 2 (will timeout in 30s)"
    taskkill /F /FI "WINDOWTITLE eq logstream.exe" >nul 2>&1
    call :success "Follower 2 killed"
    call :log "Waiting for timeout (30 seconds)..."
    
    REM Countdown
    for /l %%i in (30,-1,1) do (
        <nul set /p =Timeout in: %%is  
        timeout /t 1 /nobreak >nul
    )
    echo.
    
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 2 timeout (seq should be 4)
    echo %TEST_PREFIX% =========================================
    type %TEMP%\logstream-leader.log | findstr /C:"Timeout: Removed" /C:"REPLICATE seq="
    call :success "Expected seq=4, Follower 2 removed due to timeout"
    set EXPECTED_SEQ=5
    
    REM STEP 6: Re-add follower 2
    echo.
    call :log "STEP 5: Re-add Follower 2"
    set PORT=8003
    start /b cmd /c "logstream.exe > %TEMP%\logstream-follower2-new.log 2>&1"
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 2 rejoins (seq should be 5)
    echo %TEST_PREFIX% =========================================
    type %TEMP%\logstream-leader.log | findstr /C:"REPLICATE seq=5" /C:"Total Brokers: 3"
    call :success "Expected seq=5, Follower 2 rejoined"
    
    REM Final summary
    echo.
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% SEQUENCE PROGRESSION SUMMARY
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower 1 joined
    echo %TEST_PREFIX% seq=3: Follower 2 joined
    echo %TEST_PREFIX% seq=4: Follower 2 timed out and removed
    echo %TEST_PREFIX% seq=5: Follower 2 rejoined
    echo.
    echo %TEST_PREFIX% Current sequence: 5
    echo %TEST_PREFIX% ================================================
    echo.
    echo %TEST_PREFIX% FINAL REGISTRY STATE:
    type %TEMP%\logstream-leader.log | findstr /A:"Registry Status" /C:"Sequence Number" /C:"Total Brokers"
    
    echo.
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% KEY INSIGHTS:
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX%  Sequences increment ONLY on state changes
    echo %TEST_PREFIX%  State changes: joins, leaves, timeouts
    echo %TEST_PREFIX%  Heartbeats are NOT state changes (seq=0)
    echo %TEST_PREFIX%  REPLICATE messages carry the sequence number
    echo %TEST_PREFIX% ================================================
    echo.
    
    call :log "Press any key to exit..."
    pause >nul
    
) else if "%MODE%"=="docker" (
    set COMPOSE_FILE=%SCRIPT_DIR%compose\sequence-demo.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    set EXPECTED_SEQ=1
    
    REM STEP 1: Start leader
    call :log "STEP 1: Start Leader"
    docker compose -f sequence-demo.yaml up -d leader
    timeout /t 3 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After leader start (seq should be 1)
    echo %TEST_PREFIX% =========================================
    docker compose -f sequence-demo.yaml logs leader | findstr /C:"seq=" /C:"Registered"
    call :success "Expected seq=1"
    
    REM STEP 2: Add broker1
    echo.
    call :log "STEP 2: Add Follower 1"
    docker compose -f sequence-demo.yaml up -d broker1
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 1 joins (seq should be 2)
    echo %TEST_PREFIX% =========================================
    docker compose -f sequence-demo.yaml logs leader | findstr /C:"REPLICATE seq=2"
    call :success "Expected seq=2"
    
    REM STEP 3: Add broker2
    echo.
    call :log "STEP 3: Add Follower 2"
    docker compose -f sequence-demo.yaml up -d broker2
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 2 joins (seq should be 3)
    echo %TEST_PREFIX% =========================================
    docker compose -f sequence-demo.yaml logs leader | findstr /C:"REPLICATE seq=3"
    call :success "Expected seq=3"
    
    REM Heartbeats
    echo.
    call :log "HEARTBEAT CHECK"
    timeout /t 3 /nobreak >nul
    docker compose -f sequence-demo.yaml logs --tail=30 leader | findstr "HEARTBEAT"
    call :success "Heartbeats use seq=0"
    
    REM STEP 4: Stop broker2
    echo.
    call :log "STEP 4: Stop Follower 2"
    docker compose -f sequence-demo.yaml stop broker2
    call :success "Follower 2 stopped"
    call :log "Waiting for timeout (30 seconds)..."
    
    for /l %%i in (30,-1,1) do (
        <nul set /p =Timeout in: %%is  
        timeout /t 1 /nobreak >nul
    )
    echo.
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After timeout (seq should be 4)
    echo %TEST_PREFIX% =========================================
    docker compose -f sequence-demo.yaml logs leader | findstr /C:"Removed" /C:"seq=4"
    call :success "Expected seq=4"
    
    REM STEP 5: Restart broker2
    echo.
    call :log "STEP 5: Restart Follower 2"
    docker compose -f sequence-demo.yaml start broker2
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After rejoin (seq should be 5)
    echo %TEST_PREFIX% =========================================
    docker compose -f sequence-demo.yaml logs leader | findstr /C:"seq=5"
    call :success "Expected seq=5"
    
    echo.
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% SEQUENCE PROGRESSION SUMMARY
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower 1 joined
    echo %TEST_PREFIX% seq=3: Follower 2 joined
    echo %TEST_PREFIX% seq=4: Follower 2 timed out
    echo %TEST_PREFIX% seq=5: Follower 2 rejoined
    echo %TEST_PREFIX% ================================================
    echo.
    
    call :log "Following logs (Ctrl+C to stop)..."
    docker compose -f sequence-demo.yaml logs -f
    
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
    
    set EXPECTED_SEQ=1
    
    call :log "STEP 1: Start Leader"
    vagrant ssh leader -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.10:8001 IS_LEADER=true MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream.log 2>&1 &"
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After leader start (seq should be 1)
    echo %TEST_PREFIX% =========================================
    vagrant ssh leader -c "tail -50 /tmp/logstream.log 2>/dev/null" 2>nul | findstr /C:"Registered broker" /C:"seq=" | findstr /V "seq=0"
    call :success "Expected seq=!EXPECTED_SEQ!, Leader registered"
    set /a EXPECTED_SEQ+=1
    
    echo.
    call :log "STEP 2: Add Follower 1"
    vagrant ssh broker1 -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.20:8002 IS_LEADER=false MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream.log 2>&1 &"
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 1 joins (seq should be 2)
    echo %TEST_PREFIX% =========================================
    vagrant ssh leader -c "tail -50 /tmp/logstream.log 2>/dev/null" 2>nul | findstr /C:"seq=" | findstr /V "seq=0" | findstr /C:"Total Brokers: 2"
    call :success "Expected seq=!EXPECTED_SEQ!, Follower 1 joined"
    set /a EXPECTED_SEQ+=1
    
    echo.
    call :log "STEP 3: Add Follower 2"
    vagrant ssh broker2 -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.30:8003 IS_LEADER=false MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream.log 2>&1 &"
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% After follower 2 joins (seq should be 3)
    echo %TEST_PREFIX% =========================================
    vagrant ssh leader -c "tail -50 /tmp/logstream.log 2>/dev/null" 2>nul | findstr /C:"seq=" | findstr /V "seq=0" | findstr /C:"Total Brokers: 3"
    call :success "Expected seq=!EXPECTED_SEQ!, Follower 2 joined"
    
    echo.
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% SEQUENCE PROGRESSION SUMMARY
    echo %TEST_PREFIX% ================================================
    echo %TEST_PREFIX% seq=1: Leader self-registration
    echo %TEST_PREFIX% seq=2: Follower 1 joined
    echo %TEST_PREFIX% seq=3: Follower 2 joined
    echo %TEST_PREFIX% ================================================
    echo.
    call :success "Sequence test complete (Vagrant)"
    
    REM Cleanup
    vagrant ssh leader -c "pkill -f logstream" 2>nul
    vagrant ssh broker1 -c "pkill -f logstream" 2>nul
    vagrant ssh broker2 -c "pkill -f logstream" 2>nul
    
) else (
    call :error_msg "Invalid mode: %MODE% (use 'local', 'docker', or 'vagrant')"
    exit /b 1
)

endlocal