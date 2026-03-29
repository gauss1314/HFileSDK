@echo off
call "%~dp0_run_msys2_script.bat" build.sh %*
exit /b %ERRORLEVEL%
