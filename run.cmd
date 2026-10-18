@echo off
chcp 65001 > nul
uv run --project "%~dp0" pair_recon %*
set EXIT_CODE=%ERRORLEVEL%
if not "%EXIT_CODE%"=="0" pause
exit /b %EXIT_CODE%
