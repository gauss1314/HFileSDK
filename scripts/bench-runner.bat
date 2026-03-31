@echo off
setlocal
call "%~dp0_run_msys2_script.bat" bench-runner %*
exit /b %ERRORLEVEL%
