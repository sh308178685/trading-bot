@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 scripts\launch-martin.py %*
) else (
    python scripts\launch-martin.py %*
)

set EXIT_CODE=%errorlevel%
if not "%EXIT_CODE%"=="0" (
    echo.
    echo Martin bot exited with code %EXIT_CODE%.
    pause
)
exit /b %EXIT_CODE%
