@echo off
setlocal

set "ROOT=%~dp0"
set "CONFIG_FILE=%ROOT%projexcellent_config.json"
set "RUNNER=%ROOT%Code\new_project.py"
set "VENV_PY=%ROOT%Code\.venv\Scripts\python.exe"

if not exist "%RUNNER%" (
    echo ERROR: Could not find "%RUNNER%"
    pause
    exit /b 1
)

if not exist "%CONFIG_FILE%" (
    echo ERROR: Could not find config file "%CONFIG_FILE%"
    pause
    exit /b 1
)

if exist "%VENV_PY%" (
    set "PYTHON=%VENV_PY%"
) else (
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
            pause
            exit /b 1
        )
    )
)

cd /d "%ROOT%" || exit /b 1
%PYTHON% "%RUNNER%" --config "%CONFIG_FILE%" %*
exit /b %errorlevel%
