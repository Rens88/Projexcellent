@echo off
setlocal

rem ------------------------------------------------------------
rem run_report.bat
rem Creates/uses .venv, installs requirements, runs make_report.py
rem ------------------------------------------------------------

set "ROOT=%~dp0"
cd /d "%ROOT%" || goto :cd_error

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
        goto :fail
    )
)

if not exist ".venv\Scripts\python.exe" (
    echo Creating virtual environment in .venv...
    %PYTHON% -m venv ".venv" || goto :fail
)

call ".venv\Scripts\activate.bat" || goto :fail

echo Installing requirements (this may take a moment)...
python -m pip install -r requirements.txt --quiet || goto :fail

echo Running report...
python make_report.py || goto :fail

echo.
echo =====================================
echo FINISHED SUCCESSFULLY
echo Check the Reports folder for:
echo   YYYY-MM-DD_project_report.html
echo   YYYY-MM-DD_project_report.png
echo =====================================
goto :end

:cd_error
echo.
echo ERROR: Could not change directory to the Reports folder.
goto :end

:fail
echo.
echo =====================================
echo ERROR: Something went wrong.
echo Try deleting the .venv folder and running again.
echo =====================================

:end
echo.
echo Press any key to close this window...
pause >nul
endlocal
