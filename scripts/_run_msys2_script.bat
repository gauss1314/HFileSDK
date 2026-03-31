@echo off
setlocal
if "%~1"=="" exit /b 2
set "SCRIPT_NAME=%~1"
shift
set "SCRIPT_DIR=%~dp0"
set "TARGET_SH=%SCRIPT_DIR%%SCRIPT_NAME%.sh"
if not exist "%TARGET_SH%" (
  echo ERROR: script not found: %TARGET_SH%
  exit /b 1
)
if defined MSYS2_BASH set "BASH_EXE=%MSYS2_BASH%"
if not defined BASH_EXE if defined MSYS2_ROOT if exist "%MSYS2_ROOT%\usr\bin\bash.exe" set "BASH_EXE=%MSYS2_ROOT%\usr\bin\bash.exe"
if not defined BASH_EXE if exist "C:\msys64\usr\bin\bash.exe" set "BASH_EXE=C:\msys64\usr\bin\bash.exe"
if not defined BASH_EXE if exist "C:\msys32\usr\bin\bash.exe" set "BASH_EXE=C:\msys32\usr\bin\bash.exe"
if not defined BASH_EXE for %%I in (bash.exe) do set "BASH_EXE=%%~$PATH:I"
if not defined BASH_EXE (
  echo ERROR: bash.exe not found. Set MSYS2_BASH or MSYS2_ROOT first.
  exit /b 1
)
if not defined MSYSTEM set "MSYSTEM=CLANG64"
if not defined MSYS2_ROOT (
  for %%I in ("%BASH_EXE%") do set "BASH_DIR=%%~dpI"
  for %%I in ("%BASH_DIR%..\..") do set "MSYS2_ROOT=%%~fI"
)
if exist "%MSYS2_ROOT%\%MSYSTEM%\bin" set "PATH=%MSYS2_ROOT%\%MSYSTEM%\bin;%PATH%"
if exist "%MSYS2_ROOT%\usr\bin" set "PATH=%MSYS2_ROOT%\usr\bin;%PATH%"
pushd "%SCRIPT_DIR%\.."
"%BASH_EXE%" "scripts/%SCRIPT_NAME%.sh" %*
set "RC=%ERRORLEVEL%"
popd
exit /b %RC%
