@echo off
set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR:~0,-1%
set MINIFORGE_DIR=%PROJECT_DIR%\..\env\miniforge3

if exist "%MINIFORGE_DIR%\Scripts\activate.bat" (
    call "%MINIFORGE_DIR%\Scripts\activate.bat" "%MINIFORGE_DIR%"
)

conda activate a_share
cd /d "%PROJECT_DIR%"
cmd /k
