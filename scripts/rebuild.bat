@echo off
setlocal enabledelayedexpansion

REM Colors for Windows (using PowerShell for colored output)
set "BLUE=[94m"
set "GREEN=[92m"
set "RED=[91m"
set "YELLOW=[93m"
set "NC=[0m"

echo.
echo ========================================
echo LogStream Complete Cleanup ^& Rebuild
echo ========================================
echo.

REM Kill processes
echo [93m-^> Killing all logstream processes...[0m
taskkill /F /IM logstream.exe >nul 2>&1
taskkill /F /IM producer.exe >nul 2>&1
taskkill /F /IM consumer.exe >nul 2>&1
timeout /t 1 /nobreak >nul
echo [92m+ Processes killed[0m

REM Free ports
echo.
echo [93m-^> Freeing ports 8001-8003...[0m
for %%p in (8001 8002 8003) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%%p') do (
        if not "%%a"=="" (
            echo [91m  Killing PID %%a on port %%p[0m
            taskkill /F /PID %%a >nul 2>&1
        )
    )
    echo [92m  Port %%p is free[0m
)

REM Remove binaries
echo.
echo [93m-^> Removing old binaries...[0m
if exist logstream.exe del /F /Q logstream.exe
if exist producer.exe del /F /Q producer.exe
if exist consumer.exe del /F /Q consumer.exe
echo [92m+ Binaries removed[0m

REM Clean logs
echo.
echo [93m-^> Cleaning logs...[0m
del /F /Q %TEMP%\logstream-*.log >nul 2>&1
echo [92m+ Logs cleaned[0m

REM Rebuild all binaries
echo.
echo [93m-^> Rebuilding logstream...[0m
go build -o logstream.exe main.go
if %ERRORLEVEL% EQU 0 (
    echo [92m+ logstream.exe built successfully![0m
    dir logstream.exe | findstr logstream.exe
) else (
    echo [91mx Build failed for logstream.exe![0m
    exit /b 1
)

echo.
echo [93m-^> Rebuilding producer...[0m
go build -o producer.exe cmd\producer\main.go
if %ERRORLEVEL% EQU 0 (
    echo [92m+ producer.exe built successfully![0m
    dir producer.exe | findstr producer.exe
) else (
    echo [91mx Build failed for producer.exe![0m
    exit /b 1
)

echo.
echo [93m-^> Rebuilding consumer...[0m
go build -o consumer.exe cmd\consumer\main.go
if %ERRORLEVEL% EQU 0 (
    echo [92m+ consumer.exe built successfully![0m
    dir consumer.exe | findstr consumer.exe
) else (
    echo [91mx Build failed for consumer.exe![0m
    exit /b 1
)

echo.
echo [92m+ Ready to test![0m
echo.

endlocal