@echo off
setlocal EnableExtensions

if "%~1"=="" exit /b 2

set "TARGET_SCRIPT=%~1"
shift

if defined MSYS2_BASH if exist "%MSYS2_BASH%" goto run
if defined MSYS2_ROOT if exist "%MSYS2_ROOT%\usr\bin\bash.exe" (
  set "MSYS2_BASH=%MSYS2_ROOT%\usr\bin\bash.exe"
  goto run
)
if exist "C:\msys64\usr\bin\bash.exe" (
  set "MSYS2_BASH=C:\msys64\usr\bin\bash.exe"
  goto run
)
if exist "C:\msys32\usr\bin\bash.exe" (
  set "MSYS2_BASH=C:\msys32\usr\bin\bash.exe"
  goto run
)
for /f "delims=" %%I in ('where bash.exe 2^>nul') do (
  set "MSYS2_BASH=%%I"
  goto run
)

echo MSYS2 bash.exe not found.
echo Set MSYS2_BASH to the full path of bash.exe, or set MSYS2_ROOT to the MSYS2 installation directory.
exit /b 1

:run
if not defined MSYSTEM set "MSYSTEM=CLANG64"
if defined MSYS2_ROOT (
  if exist "%MSYS2_ROOT%\%MSYSTEM%\bin" set "PATH=%MSYS2_ROOT%\%MSYSTEM%\bin;%MSYS2_ROOT%\usr\bin;%PATH%"
)
set "CHERE_INVOKING=1"
set "MSYS2_PATH_TYPE=inherit"
"%MSYS2_BASH%" "%~dp0%TARGET_SCRIPT%" %*
exit /b %ERRORLEVEL%
