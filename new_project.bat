@echo off
setlocal

set "ROOT=%~dp0"
set "CONFIG_FILE=%ROOT%projexcellent_config.json"
set "RUNNER=%ROOT%Code\new_project.py"
set "VENV_PY=%ROOT%Code\.venv\Scripts\python.exe"
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
            set "EXIT_CODE=1"
            goto :end
        )
    )
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo ERROR: Could not change directory to "%ROOT%".
    set "EXIT_CODE=1"
    goto :end
)

if "%~1"=="" (
    echo No arguments supplied. Enter new project details.
    set "NP_COUNTER="
    set "NP_SLUG="
    set "NP_PROJECT_NAME="
    set "NP_YEAR="
    set "NP_PROGRAMMA="
    set "NP_THEME="
    set "NP_OWNER="
    set "NP_REQUESTER="
    set "NP_STATUS="
    set "NP_PRIORITY="

    set /p NP_COUNTER=Counter required, e.g. 12: 
    set /p NP_SLUG=Slug required, e.g. sleep_study: 
    set /p NP_PROJECT_NAME=Project name required: 
    set /p NP_YEAR=Year optional, default current year: 
    set /p NP_PROGRAMMA=Programma optional, default Other: 
    set /p NP_THEME=Theme optional, default General: 
    set /p NP_OWNER=Owner optional: 
    set /p NP_REQUESTER=Requester optional, default Unknown: 
    set /p NP_STATUS=Status optional [Proposed/Active/On-hold/Closed/Cancelled]: 
    set /p NP_PRIORITY=Priority optional [Low/Medium/High/Critical]: 

    if "%NP_COUNTER%"=="" (
        echo ERROR: Counter is required.
        set "EXIT_CODE=1"
        goto :end
    )
    if "%NP_SLUG%"=="" (
        echo ERROR: Slug is required.
        set "EXIT_CODE=1"
        goto :end
    )
    if "%NP_PROJECT_NAME%"=="" (
        echo ERROR: Project name is required.
        set "EXIT_CODE=1"
        goto :end
    )

    set "CMD_ARGS=--config \"%CONFIG_FILE%\" --counter \"%NP_COUNTER%\" --slug \"%NP_SLUG%\" --project-name \"%NP_PROJECT_NAME%\""
    if not "%NP_YEAR%"=="" set "CMD_ARGS=%CMD_ARGS% --year \"%NP_YEAR%\""
    if not "%NP_PROGRAMMA%"=="" set "CMD_ARGS=%CMD_ARGS% --programma \"%NP_PROGRAMMA%\""
    if not "%NP_THEME%"=="" set "CMD_ARGS=%CMD_ARGS% --theme \"%NP_THEME%\""
    if not "%NP_OWNER%"=="" set "CMD_ARGS=%CMD_ARGS% --owner \"%NP_OWNER%\""
    if not "%NP_REQUESTER%"=="" set "CMD_ARGS=%CMD_ARGS% --requester \"%NP_REQUESTER%\""
    if not "%NP_STATUS%"=="" set "CMD_ARGS=%CMD_ARGS% --status \"%NP_STATUS%\""
    if not "%NP_PRIORITY%"=="" set "CMD_ARGS=%CMD_ARGS% --priority \"%NP_PRIORITY%\""
    %PYTHON% "%RUNNER%" %CMD_ARGS%
) else (
    %PYTHON% "%RUNNER%" --config "%CONFIG_FILE%" %*
)
set "EXIT_CODE=%errorlevel%"

:end
echo.
echo Press any key to close...
pause >nul
exit /b %EXIT_CODE%
