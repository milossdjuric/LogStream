@echo off
setlocal enabledelayedexpansion

REM Test identification prefix
set TEST_PREFIX=[ELECTION]

set MODE=%1
if "%MODE%"=="" set MODE=local

echo %TEST_PREFIX% Starting LCR Election Test (mode: %MODE%)

if "%MODE%"=="local" (
    goto LOCAL_MODE
) else if "%MODE%"=="docker" (
    goto DOCKER_MODE
) else if "%MODE%"=="vagrant" (
    goto VAGRANT_MODE
) else (
    echo %TEST_PREFIX% Invalid mode: %MODE%
    echo %TEST_PREFIX% Usage: test-election.bat [local^|docker^|vagrant]
    exit /b 1
)

:LOCAL_MODE
echo %TEST_PREFIX% Testing LCR leader election with 3 local nodes
echo.

REM Cleanup existing processes
echo %TEST_PREFIX% Cleaning up old processes...
taskkill /F /IM logstream.exe >nul 2>&1
timeout /t 2 /nobreak >nul

REM Remove old logs
del /Q C:\temp\logstream-election-*.log >nul 2>&1

REM Start Node A (initial leader, lowest ID)
echo %TEST_PREFIX% Starting Node A (initial leader)...
set NODE_ADDRESS=127.0.0.1:8001
set IS_LEADER=true
set MULTICAST_GROUP=239.0.0.1:9999
set BROADCAST_PORT=8888
start "LogStream-Election-A" /MIN cmd /c "logstream.exe > C:\temp\logstream-election-a.log 2>&1"

timeout /t 3 /nobreak >nul

REM Start Node B (follower)
echo %TEST_PREFIX% Starting Node B (follower)...
set NODE_ADDRESS=127.0.0.1:8002
set IS_LEADER=false
set MULTICAST_GROUP=239.0.0.1:9999
set BROADCAST_PORT=8888
start "LogStream-Election-B" /MIN cmd /c "logstream.exe > C:\temp\logstream-election-b.log 2>&1"

timeout /t 3 /nobreak >nul

REM Start Node C (follower, will have highest ID)
echo %TEST_PREFIX% Starting Node C (follower)...
set NODE_ADDRESS=127.0.0.1:8003
set IS_LEADER=false
set MULTICAST_GROUP=239.0.0.1:9999
set BROADCAST_PORT=8888
start "LogStream-Election-C" /MIN cmd /c "logstream.exe > C:\temp\logstream-election-c.log 2>&1"

echo %TEST_PREFIX% All nodes started. Waiting for cluster formation...
timeout /t 8 /nobreak >nul

REM Check processes are running
tasklist /FI "IMAGENAME eq logstream.exe" 2>NUL | find /I /N "logstream.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo %TEST_PREFIX% [SUCCESS] All nodes are running
) else (
    echo %TEST_PREFIX% [ERROR] Some nodes failed to start
    goto CLEANUP_LOCAL
)

REM Extract Node IDs
echo %TEST_PREFIX% Extracting node IDs...
for /f "tokens=3" %%i in ('findstr /C:"Node ID:" C:\temp\logstream-election-a.log') do set NODE_A_ID=%%i
for /f "tokens=3" %%i in ('findstr /C:"Node ID:" C:\temp\logstream-election-b.log') do set NODE_B_ID=%%i
for /f "tokens=3" %%i in ('findstr /C:"Node ID:" C:\temp\logstream-election-c.log') do set NODE_C_ID=%%i

echo %TEST_PREFIX% Node A ID: %NODE_A_ID%
echo %TEST_PREFIX% Node B ID: %NODE_B_ID%
echo %TEST_PREFIX% Node C ID: %NODE_C_ID%

REM Determine highest ID (simple comparison for demo)
echo %TEST_PREFIX% Expected winner will be determined by string comparison
echo.

REM Manual trigger note
echo %TEST_PREFIX% ============================================
echo %TEST_PREFIX% MANUAL ELECTION TRIGGER
echo %TEST_PREFIX% ============================================
echo %TEST_PREFIX% 
echo %TEST_PREFIX% To trigger election, open a new terminal and type:
echo %TEST_PREFIX%   echo election ^| nc 127.0.0.1 8001
echo %TEST_PREFIX% 
echo %TEST_PREFIX% Or manually connect to Node A and type "election"
echo %TEST_PREFIX% 
echo %TEST_PREFIX% Press any key after triggering election...
pause

echo %TEST_PREFIX% Waiting for election to complete (15 seconds)...
timeout /t 15 /nobreak >nul

REM Check results
echo %TEST_PREFIX% Checking election results...
echo.

findstr /C:"Message returned to me - I WIN" C:\temp\logstream-election-a.log >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo %TEST_PREFIX% [SUCCESS] Node A won the election!
)

findstr /C:"Message returned to me - I WIN" C:\temp\logstream-election-b.log >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo %TEST_PREFIX% [SUCCESS] Node B won the election!
)

findstr /C:"Message returned to me - I WIN" C:\temp\logstream-election-c.log >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo %TEST_PREFIX% [SUCCESS] Node C won the election!
)

REM Show election flow
echo.
echo %TEST_PREFIX% Election message flow:
echo ----------------------------------------
findstr /C:"ELECTION" C:\temp\logstream-election-*.log | more
echo ----------------------------------------

echo.
echo %TEST_PREFIX% [SUCCESS] Election test completed!
echo.
echo %TEST_PREFIX% Logs saved to:
echo %TEST_PREFIX%   - C:\temp\logstream-election-a.log
echo %TEST_PREFIX%   - C:\temp\logstream-election-b.log
echo %TEST_PREFIX%   - C:\temp\logstream-election-c.log

:CLEANUP_LOCAL
echo.
echo %TEST_PREFIX% Cleaning up processes...
taskkill /F /IM logstream.exe >nul 2>&1
goto END

:DOCKER_MODE
echo %TEST_PREFIX% Testing LCR leader election with Docker
echo.

set COMPOSE_FILE=%~dp0..\compose\election.yaml

if not exist "%COMPOSE_FILE%" (
    echo %TEST_PREFIX% [ERROR] Compose file not found: %COMPOSE_FILE%
    exit /b 1
)

echo %TEST_PREFIX% Using compose file: %COMPOSE_FILE%

REM Cleanup existing containers
echo %TEST_PREFIX% Cleaning up old containers...
docker compose -f "%COMPOSE_FILE%" down >nul 2>&1

REM Start containers
echo %TEST_PREFIX% Starting 3-node cluster...
docker compose -f "%COMPOSE_FILE%" up -d

echo %TEST_PREFIX% Waiting for cluster to form (15 seconds)...
timeout /t 15 /nobreak >nul

REM Check container status
echo %TEST_PREFIX% Checking container status...
docker compose -f "%COMPOSE_FILE%" ps

REM Get Node IDs
echo %TEST_PREFIX% Extracting node IDs...
for /f "tokens=3" %%i in ('docker logs logstream-election-a 2^>^&1 ^| findstr /C:"Node ID:"') do set NODE_A_ID=%%i
for /f "tokens=3" %%i in ('docker logs logstream-election-b 2^>^&1 ^| findstr /C:"Node ID:"') do set NODE_B_ID=%%i
for /f "tokens=3" %%i in ('docker logs logstream-election-c 2^>^&1 ^| findstr /C:"Node ID:"') do set NODE_C_ID=%%i

echo %TEST_PREFIX% Node A ID: %NODE_A_ID%
echo %TEST_PREFIX% Node B ID: %NODE_B_ID%
echo %TEST_PREFIX% Node C ID: %NODE_C_ID%

echo.
echo %TEST_PREFIX% ============================================
echo %TEST_PREFIX% MANUAL ELECTION TRIGGER
echo %TEST_PREFIX% ============================================
echo %TEST_PREFIX% 
echo %TEST_PREFIX% To trigger election, run:
echo %TEST_PREFIX%   docker exec -it logstream-election-a sh
echo %TEST_PREFIX%   Then type: election
echo %TEST_PREFIX% 
echo %TEST_PREFIX% Press any key after triggering election...
pause

echo %TEST_PREFIX% Waiting for election to complete (20 seconds)...
timeout /t 20 /nobreak >nul

REM Check results
echo %TEST_PREFIX% Checking election results...
docker logs logstream-election-a 2>&1 | findstr /C:"Message returned to me - I WIN" >nul
if %ERRORLEVEL% EQU 0 echo %TEST_PREFIX% [SUCCESS] Node A won the election!

docker logs logstream-election-b 2>&1 | findstr /C:"Message returned to me - I WIN" >nul
if %ERRORLEVEL% EQU 0 echo %TEST_PREFIX% [SUCCESS] Node B won the election!

docker logs logstream-election-c 2>&1 | findstr /C:"Message returned to me - I WIN" >nul
if %ERRORLEVEL% EQU 0 echo %TEST_PREFIX% [SUCCESS] Node C won the election!

echo.
echo %TEST_PREFIX% Election message flow:
echo ----------------------------------------
docker logs logstream-election-a 2>&1 | findstr /C:"ELECTION"
docker logs logstream-election-b 2>&1 | findstr /C:"ELECTION"
docker logs logstream-election-c 2>&1 | findstr /C:"ELECTION"
echo ----------------------------------------

echo.
echo %TEST_PREFIX% [SUCCESS] Docker election test completed!
echo.
echo %TEST_PREFIX% Containers are still running. To view logs:
echo %TEST_PREFIX%   docker logs logstream-election-a
echo %TEST_PREFIX%   docker logs logstream-election-b
echo %TEST_PREFIX%   docker logs logstream-election-c
echo.
echo %TEST_PREFIX% To stop containers:
echo %TEST_PREFIX%   docker compose -f "%COMPOSE_FILE%" down
goto END

:VAGRANT_MODE
echo %TEST_PREFIX% Testing LCR leader election with 3 Vagrant VMs
echo.

set PROJECT_ROOT=%~dp0..\..

cd /d "%PROJECT_ROOT%\deploy\vagrant"

echo %TEST_PREFIX% Checking Vagrant VMs...
for %%v in (leader broker1 broker2) do (
    vagrant status %%v 2>nul | findstr /C:"running" >nul
    if errorlevel 1 (
        echo %TEST_PREFIX% ERROR: %%v VM not running! Run: vagrant up
        exit /b 1
    )
)

echo %TEST_PREFIX% Starting Node A (initial leader)...
vagrant ssh leader -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.10:8001 IS_LEADER=true MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream-election-a.log 2>&1 &"
timeout /t 3 /nobreak >nul

echo %TEST_PREFIX% Starting Node B (follower)...
vagrant ssh broker1 -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.20:8002 IS_LEADER=false MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream-election-b.log 2>&1 &"
timeout /t 3 /nobreak >nul

echo %TEST_PREFIX% Starting Node C (follower)...
vagrant ssh broker2 -c "cd /vagrant/logstream && NODE_ADDRESS=192.168.56.30:8003 IS_LEADER=false MULTICAST_GROUP=239.0.0.1:9999 BROADCAST_PORT=8888 nohup ./logstream > /tmp/logstream-election-c.log 2>&1 &"
timeout /t 5 /nobreak >nul

echo %TEST_PREFIX% Waiting for cluster to stabilize...
timeout /t 10 /nobreak >nul

echo %TEST_PREFIX% Stopping leader (Node A) to trigger election...
vagrant ssh leader -c "pkill -f logstream"
timeout /t 5 /nobreak >nul

echo %TEST_PREFIX% Triggering manual election on Node B...
vagrant ssh broker1 -c "pkill -USR1 -f logstream"
timeout /t 15 /nobreak >nul

echo.
echo %TEST_PREFIX% Checking election results...
vagrant ssh broker1 -c "grep -E 'Message returned to me - I WIN|VICTORY message completed circuit' /tmp/logstream-election-b.log 2>/dev/null" 2>nul | findstr /C:"WIN" /C:"VICTORY" >nul
if %ERRORLEVEL% EQU 0 echo %TEST_PREFIX% [SUCCESS] Node B won the election!

vagrant ssh broker2 -c "grep -E 'Message returned to me - I WIN|VICTORY message completed circuit' /tmp/logstream-election-c.log 2>/dev/null" 2>nul | findstr /C:"WIN" /C:"VICTORY" >nul
if %ERRORLEVEL% EQU 0 echo %TEST_PREFIX% [SUCCESS] Node C won the election!

echo.
echo %TEST_PREFIX% Election message flow:
echo ----------------------------------------
vagrant ssh broker1 -c "grep ELECTION /tmp/logstream-election-b.log 2>/dev/null" 2>nul
vagrant ssh broker2 -c "grep ELECTION /tmp/logstream-election-c.log 2>/dev/null" 2>nul
echo ----------------------------------------

echo.
echo %TEST_PREFIX% [SUCCESS] Vagrant election test completed!
echo.
echo %TEST_PREFIX% To view logs:
echo %TEST_PREFIX%   vagrant ssh leader -c "cat /tmp/logstream-election-a.log"
echo %TEST_PREFIX%   vagrant ssh broker1 -c "cat /tmp/logstream-election-b.log"
echo %TEST_PREFIX%   vagrant ssh broker2 -c "cat /tmp/logstream-election-c.log"
echo.
echo %TEST_PREFIX% Cleaning up...
vagrant ssh leader -c "pkill -f logstream" 2>nul
vagrant ssh broker1 -c "pkill -f logstream" 2>nul
vagrant ssh broker2 -c "pkill -f logstream" 2>nul

:END
endlocal