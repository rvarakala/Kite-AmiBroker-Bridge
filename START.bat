@echo off
title Kite - AmiBroker Bridge
cd /d "%~dp0"
where python >nul 2>nul || (echo Python is not installed. Get it from python.org and tick "Add python.exe to PATH". & pause & exit /b)
if not exist .venv (
  echo First run: setting up ^(one minute^)...
  python -m venv .venv || (pause & exit /b)
)
call .venv\Scripts\activate.bat
python -m pip install -q --disable-pip-version-check -r requirements.txt || (echo Package install failed. & pause & exit /b)
python bridge.py
pause
