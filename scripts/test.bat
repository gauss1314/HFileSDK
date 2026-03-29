@echo off
call "%~dp0_run_msys2_script.bat" test.sh %*
exit /b %ERRORLEVEL%
