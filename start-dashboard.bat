@echo off
setlocal
cd /d "%~dp0"

set "VENV_PYTHON=%CD%\.venv\Scripts\python.exe"
if not exist "%VENV_PYTHON%" (
    where python >nul 2>nul || (
        echo Python 3 was not found in PATH.
        exit /b 1
    )
    echo Creating virtual environment in .venv...
    python -m venv .venv || exit /b 1
    "%VENV_PYTHON%" -m pip install --upgrade pip || exit /b 1
    "%VENV_PYTHON%" -m pip install -r requirements.txt || exit /b 1
)

"%VENV_PYTHON%" scripts\launch-dashboard.py %*

set EXIT_CODE=%errorlevel%
if not "%EXIT_CODE%"=="0" (
    echo.
    echo Dashboard exited with code %EXIT_CODE%.
    pause
)
exit /b %EXIT_CODE%
