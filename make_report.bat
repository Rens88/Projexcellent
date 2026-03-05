@echo off
setlocal

set "ROOT=%~dp0"
set "CONFIG_FILE=%ROOT%projexcellent_config.json"
set "RUNNER=%ROOT%Code\run_report.py"
set "EXIT_CODE=0"

if not exist "%RUNNER%" (
    echo ERROR: Could not find "%RUNNER%"
    set "EXIT_CODE=1"
    goto :end
)

if not exist "%CONFIG_FILE%" (
    echo ERROR: Could not find config file "%CONFIG_FILE%"
    set "EXIT_CODE=1"
    goto :end
)

set "PYTHON="
py -3 -c "import sys" >nul 2>&1
if %errorlevel%==0 (
    set "PYTHON=py -3"
) else (
    python -c "import sys" >nul 2>&1
    if %errorlevel%==0 (
        set "PYTHON=python"
    ) else (
        echo Python 3 is required but was not found.
        set "EXIT_CODE=1"
        goto :end
    )
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo ERROR: Could not change directory to "%ROOT%".
    set "EXIT_CODE=1"
    goto :end
)

%PYTHON% "%RUNNER%" --config "%CONFIG_FILE%" %*
set "EXIT_CODE=%errorlevel%"

:end
echo.
echo Press any key to close...
pause >nul
exit /b %EXIT_CODE%
