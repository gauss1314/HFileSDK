@echo off
call "%~dp0_run_msys2_script.bat" bench-runner.sh %*
exit /b %ERRORLEVEL%
