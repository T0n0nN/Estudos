@echo off
setlocal enableextensions

rem Run the AnyLAN mapping watcher without PowerShell execution-policy issues.
rem Usage:
rem   run_watch.bat
rem   run_watch.bat "Cable 11 and 12"
rem   run_watch.bat --once
rem   run_watch.bat --once "Cable 11 and 12"
rem   run_watch.bat --all
rem   run_watch.bat --all "Cable 11 and 12"

set "HERE=%~dp0"
set "WATCH_ALL="
set "SHEET="
set "MODE_WATCH=1"

rem Default output directory (Topology). If it exists, write the .xlsx there.
if not defined TOPOLOGY_DIR set "TOPOLOGY_DIR=C:\Users\za68397\Goodyear\Americas IT - LA Stefanini - Network Administrator L3\Network LA\Sites\Brazil\Americana\Topology"
set "OUT_XLSX=%HERE%AnyLAN_Mapping.xlsx"
pushd "%TOPOLOGY_DIR%" >nul 2>&1
if %errorlevel%==0 (
  popd
  set "OUT_XLSX=%TOPOLOGY_DIR%\AnyLAN_Mapping.xlsx"
)

if /I "%~1"=="--once" (
  set "MODE_WATCH=0"
  shift
)

if /I "%~1"=="--all" (
  set "WATCH_ALL=--watch-all"
  shift
)

if not "%~1"=="" set "SHEET=%~1"

set "PY_EXE=%HERE%\.venv\Scripts\python.exe"
set "PY_ARGS="
if exist "%PY_EXE%" goto :run

set "PY_EXE=%HERE%..\.venv\Scripts\python.exe"
set "PY_ARGS="
if exist "%PY_EXE%" goto :run

where python >nul 2>&1
if %errorlevel%==0 (
  set "PY_EXE=python"
  set "PY_ARGS="
  goto :run
)

where py >nul 2>&1
if %errorlevel%==0 (
  set "PY_EXE=py"
  set "PY_ARGS=-3"
  goto :run
)

echo Python nao encontrado. Instale Python ou mantenha o .venv ao lado da pasta excel_mapping.
echo.
pause
exit /b 1

:run
cd /d "%HERE%"

if "%MODE_WATCH%"=="1" (
  set "MODE_ARGS=--watch --poll 1.0 %WATCH_ALL%"
) else (
  set "MODE_ARGS="
)

echo Running: "%PY_EXE%" %PY_ARGS% "%HERE%generate_mapping_excel.py" %MODE_ARGS% --data "%HERE%mapping_data.json" --out "%OUT_XLSX%"
echo Output:  "%OUT_XLSX%"
if not "%SHEET%"=="" echo   with: --sheet "%SHEET%"
echo.

if "%SHEET%"=="" (
  call "%PY_EXE%" %PY_ARGS% "%HERE%generate_mapping_excel.py" %MODE_ARGS% --data "%HERE%mapping_data.json" --out "%OUT_XLSX%"
) else (
  call "%PY_EXE%" %PY_ARGS% "%HERE%generate_mapping_excel.py" %MODE_ARGS% --data "%HERE%mapping_data.json" --out "%OUT_XLSX%" --sheet "%SHEET%"
)

set "EXITCODE=%errorlevel%"
if not "%EXITCODE%"=="0" (
  echo.
  echo ERROR: o watcher terminou com codigo %EXITCODE%.
  echo (Deixe esta janela aberta e copie a mensagem acima.)
  echo.
  if not defined NO_PAUSE pause
) else (
  if "%MODE_WATCH%"=="0" (
    echo.
    echo OK (modo --once finalizado).
    if not defined NO_PAUSE pause
  )
)

endlocal
