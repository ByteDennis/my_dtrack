@echo off
chcp 65001 > nul
uv run --extra dbx python "%~dp0src\launch.py" %*
set EXIT_CODE=%ERRORLEVEL%
if not "%EXIT_CODE%"=="0" pause
exit /b %EXIT_CODE%
