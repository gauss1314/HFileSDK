@echo off
setlocal
call "%~dp0_run_msys2_script.bat" test %*
exit /b %ERRORLEVEL%
