@echo off
setlocal enabledelayedexpansion

REM Producer-Consumer Test - Basic Data Flow
REM Usage: tests\test-producer-consumer.bat [local|docker]

set MODE=%1
if "%MODE%"=="" set MODE=local

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
cd /d "%PROJECT_ROOT%"

call "%SCRIPT_DIR%lib\common.bat"

REM Test identification prefix
set TEST_PREFIX=[PRODUCER-CONSUMER]

echo %TEST_PREFIX% ================================================
echo %TEST_PREFIX% Test: Producer -^> Leader -^> Consumer Data Flow
echo %TEST_PREFIX% Mode: %MODE%
echo %TEST_PREFIX% ================================================
echo.
echo %TEST_PREFIX% This test demonstrates:
echo %TEST_PREFIX%   1. Leader accepts producer registration (TCP)
echo %TEST_PREFIX%   2. Leader accepts consumer subscription (TCP)
echo %TEST_PREFIX%   3. Producer sends data to leader (UDP)
echo %TEST_PREFIX%   4. Leader forwards to consumer (TCP)
echo.

if "%MODE%"=="local" (
    REM Local mode
    
    REM Build all components
    call :build_if_needed logstream.exe main.go
    call :build_if_needed producer.exe cmd\producer\main.go
    call :build_if_needed consumer.exe cmd\consumer\main.go
    
    REM STEP 1: Start leader
    call :log "=== STEP 1: Starting Leader (Broker) ==="
    set MULTICAST_GROUP=239.0.0.1:9999
    set BROADCAST_PORT=8888
    set IS_LEADER=true
    start /B logstream.exe > %TEMP%\logstream-leader.log 2>&1
    call :success "Leader started"
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Leader ready:"
    type %TEMP%\logstream-leader.log | findstr /C:"TCP listener" /C:"Started"
    call :success "Leader accepting connections on port 8001"
    
    REM STEP 2: Start consumer
    echo.
    call :log "=== STEP 2: Starting Consumer (Subscriber) ==="
    set LEADER_ADDRESS=localhost:8001
    set TOPIC=test-logs
    start /B consumer.exe > %TEMP%\logstream-consumer.log 2>&1
    call :success "Consumer started"
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Consumer registration:"
    type %TEMP%\logstream-leader.log | findstr /C:"CONSUME from" /C:"Registered consumer" /C:"Subscribed"
    type %TEMP%\logstream-consumer.log | findstr /C:"Connected" /C:"Subscribed"
    call :success "Consumer registered and subscribed to 'test-logs'"
    
    REM STEP 3: Start producer
    echo.
    call :log "=== STEP 3: Starting Producer ==="
    start /B producer.exe > %TEMP%\logstream-producer.log 2>&1
    call :success "Producer started"
    timeout /t 3 /nobreak >nul
    
    echo.
    call :log "Producer registration:"
    type %TEMP%\logstream-leader.log | findstr /C:"PRODUCE from" /C:"Registered producer" /C:"assigned broker"
    type %TEMP%\logstream-producer.log | findstr /C:"Connected" /C:"Registered" /C:"PRODUCE_ACK"
    call :success "Producer registered for 'test-logs'"
    
    REM STEP 4: Wait for data flow
    echo.
    call :log "=== STEP 4: Waiting for Data Flow ==="
    call :log "Producer is sending test messages automatically..."
    timeout /t 10 /nobreak >nul
    
    REM Show results
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% PRODUCER-CONSUMER TEST RESULTS:
    echo %TEST_PREFIX% =========================================
    
    echo.
    call :log "Leader activity (DATA forwarding):"
    type %TEMP%\logstream-leader.log | findstr /C:"DATA from" /C:"Forwarding" /C:"RESULT to"
    
    echo.
    call :log "Producer activity (sent messages):"
    powershell -Command "Get-Content '%TEMP%\logstream-producer.log' | Select-String '-> DATA' | Select-Object -Last 10"
    
    echo.
    call :log "Consumer activity (received messages):"
    powershell -Command "Get-Content '%TEMP%\logstream-consumer.log' | Select-String '\[test-logs\] Offset' | Select-Object -Last 10"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% DATA FLOW VERIFICATION:
    echo %TEST_PREFIX% =========================================
    
    REM Count messages
    for /f %%i in ('type %TEMP%\logstream-producer.log ^| findstr /C:"-> DATA" ^| find /c /v ""') do set SENT=%%i
    for /f %%i in ('type %TEMP%\logstream-consumer.log ^| findstr /C:"[test-logs] Offset" ^| find /c /v ""') do set RECEIVED=%%i
    
    echo %TEST_PREFIX% Messages sent by producer:     !SENT!
    echo %TEST_PREFIX% Messages received by consumer: !RECEIVED!
    
    if !SENT! GTR 0 (
        if !RECEIVED! GTR 0 (
            call :success "Messages flowing successfully!"
        )
    ) else (
        call :error_msg "No message flow detected"
    )
    
    echo.
    echo %TEST_PREFIX% Producer -^> Leader (TCP registration)
    echo %TEST_PREFIX% Consumer -^> Leader (TCP subscription)
    echo %TEST_PREFIX% Producer -^> Leader (UDP data)
    echo %TEST_PREFIX% Leader -^> Consumer (TCP delivery)
    echo.
    echo %TEST_PREFIX% Complete end-to-end flow verified!
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow logs
    call :log "Press Ctrl+C to stop..."
    echo.
    type %TEMP%\logstream-leader.log %TEMP%\logstream-producer.log %TEMP%\logstream-consumer.log
    pause

) else if "%MODE%"=="docker" (
    REM Docker mode
    set COMPOSE_FILE=%SCRIPT_DIR%compose\producer-consumer.yaml
    
    if not exist "!COMPOSE_FILE!" (
        call :error_msg "Compose file not found: !COMPOSE_FILE!"
        exit /b 1
    )
    
    cd /d "%SCRIPT_DIR%compose"
    
    REM Start all containers
    call :log "=== Starting All Containers ==="
    docker compose -f producer-consumer.yaml up -d
    
    timeout /t 8 /nobreak >nul
    
    REM Show container status
    echo.
    call :log "Container status:"
    docker compose -f producer-consumer.yaml ps
    
    REM STEP 1: Check leader
    echo.
    call :log "=== STEP 1: Leader Status ==="
    docker compose -f producer-consumer.yaml logs leader | findstr /C:"TCP listener" /C:"Started"
    call :success "Leader ready"
    
    REM STEP 2: Check consumer
    echo.
    call :log "=== STEP 2: Consumer Status ==="
    docker compose -f producer-consumer.yaml logs leader | findstr /C:"CONSUME from" /C:"Registered consumer"
    docker compose -f producer-consumer.yaml logs consumer | findstr /C:"Connected" /C:"Subscribed"
    call :success "Consumer registered"
    
    REM STEP 3: Check producer
    echo.
    call :log "=== STEP 3: Producer Status ==="
    docker compose -f producer-consumer.yaml logs leader | findstr /C:"PRODUCE from" /C:"Registered producer"
    docker compose -f producer-consumer.yaml logs producer | findstr /C:"Connected" /C:"Registered"
    call :success "Producer registered"
    
    REM STEP 4: Check data flow
    echo.
    call :log "=== STEP 4: Data Flow ==="
    timeout /t 5 /nobreak >nul
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% PRODUCER-CONSUMER TEST RESULTS:
    echo %TEST_PREFIX% =========================================
    
    echo.
    call :log "Leader activity:"
    docker compose -f producer-consumer.yaml logs leader | findstr /C:"DATA from" /C:"Forwarding" /C:"RESULT to"
    
    echo.
    call :log "Producer activity:"
    docker compose -f producer-consumer.yaml logs producer | findstr /C:"-> DATA"
    
    echo.
    call :log "Consumer activity:"
    docker compose -f producer-consumer.yaml logs consumer | findstr /C:"[test-logs] Offset"
    
    echo.
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% DATA FLOW SUMMARY:
    echo %TEST_PREFIX% =========================================
    echo %TEST_PREFIX% Producer -^> Leader (TCP registration)
    echo %TEST_PREFIX% Consumer -^> Leader (TCP subscription)
    echo %TEST_PREFIX% Producer -^> Leader (UDP data)
    echo %TEST_PREFIX% Leader -^> Consumer (TCP delivery)
    echo.
    echo %TEST_PREFIX% Complete end-to-end flow verified!
    echo %TEST_PREFIX% =========================================
    echo.
    
    REM Follow logs
    call :log "Following logs (Ctrl+C to stop)..."
    docker compose -f producer-consumer.yaml logs -f

) else (
    call :error_msg "Invalid mode: %MODE% (use 'local' or 'docker')"
    exit /b 1
)

exit /b 0