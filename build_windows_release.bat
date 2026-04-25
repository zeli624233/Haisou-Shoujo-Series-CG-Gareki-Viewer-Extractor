@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set APP_NAME=HaisonShoujoViewerExtractor
set RELEASE_NAME=HaisonShoujoViewerExtractor_Ver1.0_Windows

echo [1/6] Checking Python...
python --version || goto :error

echo [2/6] Creating virtual environment...
if not exist .venv (
    python -m venv .venv || goto :error
)
call .venv\Scripts\activate.bat || goto :error

echo [3/6] Installing dependencies...
python -m pip install --upgrade pip || goto :error
python -m pip install -r requirements.txt || goto :error
python -m pip install -r requirements-build.txt || goto :error

echo [4/6] Cleaning old build files...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist release rmdir /s /q release
mkdir release

echo [5/6] Building Windows EXE with PyInstaller...
python -m PyInstaller HaisonShoujoViewerExtractor.spec --clean --noconfirm || goto :error

echo [6/6] Packaging release folder...
copy README.md dist\%APP_NAME%\README.md >nul
copy README_中文.md dist\%APP_NAME%\README_中文.md >nul
copy LICENSE dist\%APP_NAME%\LICENSE >nul
xcopy dist\%APP_NAME% release\%RELEASE_NAME%\ /E /I /Y >nul
powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path 'release\%RELEASE_NAME%' -DestinationPath 'release\%RELEASE_NAME%.zip' -Force" || goto :error

echo.
echo Build finished:
echo   release\%RELEASE_NAME%\%APP_NAME%.exe
echo   release\%RELEASE_NAME%.zip
echo.
pause
exit /b 0

:error
echo.
echo Build failed. Please check the error messages above.
echo.
pause
exit /b 1
