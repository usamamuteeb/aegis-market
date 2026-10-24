@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" py -3 -m venv .venv
if errorlevel 1 goto failed
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto failed
".venv\Scripts\python.exe" Generate-Settings.py
if errorlevel 1 goto failed
echo Open cloud-login.txt for your username and password.
echo Import .env.production.local into Vercel Environment Variables, then add DATABASE_URL.
pause
exit /b
:failed
echo Setup did not complete. Read the error above before trying again.
pause
