@echo off
call "%~dp0_run_msys2_script.bat" coverage.sh %*
exit /b %ERRORLEVEL%
