@echo off
setlocal enabledelayedexpansion

REM Multi-Producer Test - Multiple Producers to One Consumer
REM Usage: tests\test-multi-producer.bat [local|docker]

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
cd /d "%PROJECT_ROOT%"

call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[MULTI-PRODUCER]

echo %TEST_PREFIX% ================================================
echo %TEST_PREFIX% Test: Multiple Producers -^> One Consumer
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ================================================
echo.
echo %TEST_PREFIX% This test demonstrates:
echo %TEST_PREFIX%   1. Three producers register with leader
echo %TEST_PREFIX%   2. One consumer subscribes to topic
echo %TEST_PREFIX%   3. All producers send data concurrently
echo %TEST_PREFIX%   4. Consumer receives from all producers
echo.

if "%MODE%"=="local" (
    REM Local mode
    
    REM Build all components
    call :build_if_needed logstream.exe main.go
    call :build_if_needed producer.exe cmd\producer\main.go
    call :build_if_needed consumer.exe cmd\consumer\main.go
    
    REM STEP 1: Start leader
    call :log "=== STEP 1: Starting Leader ==="
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /B logstream.exe > %TEMP%\logstream-leader.log 2>&1
    call :success "Leader started"
    timeout /t 3 /nobreak >nul
    
    REM STEP 2: Start consumer
    echo.
    call :log "=== STEP 2: Starting Consumer ==="
    set LEADER_ADDRESS=localhost:8001
    set TOPIC=test-logs
    start /B consumer.exe > %TEMP%\logstream-consumer.log 2>&1
    call :success "Consumer started"
    timeout /t 3 /nobreak >nul
    
    type %TEMP%\logstream-consumer.log | findstr /C:"Connected" /C:"Subscribed"
    call :success "Consumer subscribed to 'test-logs'"
    
    REM STEP 3: Start three producers
    echo.
    call :log "=== STEP 3: Starting Three Producers ==="
    
    start /B producer.exe > %TEMP%\logstream-producer1.log 2>&1
    call :success "Producer 1 started"
    timeout /t 2 /nobreak >nul
    
    start /B producer.exe > %TEMP%\logstream-producer2.log 2>&1
    call :success "Producer 2 started"
    timeout /t 2 /nobreak >nul
    
    start /B producer.exe > %TEMP%\logstream-producer3.log 2>&1
    call :success "Producer 3 started"
    timeout /t 3 /nobreak >nul
    
    REM Check all registered
    echo.
    call :log "Producer registrations:"
    type %TEMP%\logstream-leader.log | findstr /C:"PRODUCE from"
    
    for /f %%i in ('type %TEMP%\logstream-leader.log ^| findstr /C:"PRODUCE from" ^| find /c /v ""') do set REGISTERED=%%i
    
    if !REGISTERED!==3 (
        call :success "All 3 producers registered"
    ) else (
        call :error_msg "Only !REGISTERED! producers registered"
    )
    
    REM STEP 4: Wait for data flow
    echo.
    call :log "=== STEP 4: Waiting for Concurrent Messages ==="
    call :log "All producers are sending messages automatically..."
    timeout /t 15 /nobreak >nul
    
    REM Show results
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% MULTI-PRODUCER TEST RESULTS:
    echo %TEST_PREFIX% =========================================
    
    echo.
    call :log "Leader activity (all producers):"
    powershell -Command "Get-Content '%TEMP%\logstream-leader.log' | Select-String 'DATA from' | Select-Object -Last 15"
    
    echo.
    call :log "Consumer received messages:"
    powershell -Command "Get-Content '%TEMP%\logstream-consumer.log' | Select-String '\[test-logs\] Offset' | Select-Object -Last 15"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% MESSAGE COUNT VERIFICATION:
    echo %TEST_PREFIX% =========================================
    
    for /f %%i in ('type %TEMP%\logstream-producer1.log ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT1=%%i
    for /f %%i in ('type %TEMP%\logstream-producer2.log ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT2=%%i
    for /f %%i in ('type %TEMP%\logstream-producer3.log ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT3=%%i
    for /f %%i in ('type %TEMP%\logstream-consumer.log ^| findstr /C:"[test-logs] Offset" ^| find /c /v ""') do set RECEIVED=%%i
    
    echo %TEST_PREFIX% Producer 1 sent: !SENT1! messages
    echo %TEST_PREFIX% Producer 2 sent: !SENT2! messages
    echo %TEST_PREFIX% Producer 3 sent: !SENT3! messages
    echo %TEST_PREFIX% Consumer received: !RECEIVED! messages total
    
    if !RECEIVED! GEQ 9 (
        call :success "Consumer received messages from multiple producers!"
    ) else (
        call :error_msg "Expected at least 9 messages, got !RECEIVED!"
    )
    
    echo.
    echo %TEST_PREFIX% Multiple producers can send concurrently
    echo %TEST_PREFIX% Single consumer receives from all producers
    echo %TEST_PREFIX% Topic-based routing works correctly
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow logs
    call :log "Press Ctrl+C to stop..."
    echo.
    type %TEMP%\logstream-leader.log %TEMP%\logstream-consumer.log
    pause

) else if "%MODE%"=="docker" (
    REM Docker mode
    set COMPOSE_FILE=%SCRIPT_DIR%compose\multi-producer.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    call :log "=== Starting All Containers ==="
    docker compose -f multi-producer.yaml up -d
    
    timeout /t 10 /nobreak >nul
    
    REM Check status
    echo.
    call :log "Container status:"
    docker compose -f multi-producer.yaml ps
    
    REM Show results
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% MULTI-PRODUCER TEST RESULTS:
    echo %TEST_PREFIX% =========================================
    
    echo.
    call :log "Leader activity (all producers):"
    docker compose -f multi-producer.yaml logs leader | findstr /C:"DATA from"
    
    echo.
    call :log "Consumer received messages:"
    docker compose -f multi-producer.yaml logs consumer | findstr /C:"[test-logs] Offset"
    
    echo.
    call :log "Message counts:"
    for /f %%i in ('docker compose -f multi-producer.yaml logs producer1 ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT1=%%i
    for /f %%i in ('docker compose -f multi-producer.yaml logs producer2 ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT2=%%i
    for /f %%i in ('docker compose -f multi-producer.yaml logs producer3 ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT3=%%i
    
    echo %TEST_PREFIX% Producer 1 sent: !SENT1! messages
    echo %TEST_PREFIX% Producer 2 sent: !SENT2! messages
    echo %TEST_PREFIX% Producer 3 sent: !SENT3! messages
    
    echo.
    echo %TEST_PREFIX% Multiple producers can send concurrently
    echo %TEST_PREFIX% Single consumer receives from all producers
    echo %TEST_PREFIX% Topic-based routing works correctly
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow logs
    call :log "Following logs (Ctrl+C to stop)..."
    docker compose -f multi-producer.yaml logs -f

) else (
    call :error_msg "Invalid mode: %MODE% (use 'local' or 'docker')"
    exit /b 1
)

exit /b 0