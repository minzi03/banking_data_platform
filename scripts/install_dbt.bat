@echo off
REM =============================================================================
REM Installation Script — dbt for Banking Data Platform (Windows)
REM =============================================================================
REM Architecture: Lakehouse 2.0
REM Usage: scripts\install_dbt.bat
REM =============================================================================

echo ==========================================
echo Installing dbt for Banking Data Platform
echo ==========================================

REM Check Python version
python --version
if errorlevel 1 (
    echo Error: Python not found. Please install Python 3.8+.
    exit /b 1
)

REM Check pip
pip --version
if errorlevel 1 (
    echo Error: pip not found. Please install pip.
    exit /b 1
)

REM Install dbt and dependencies
echo.
echo Installing dbt and dependencies...
pip install --upgrade pip
pip install ^
    dbt-core==1.12.0 ^
    dbt-trino==1.8.0

echo.
echo dbt installed successfully!

REM Verify installation
echo.
echo Verifying installation...
dbt --version

REM Navigate to dbt project directory
cd /d "%~dp0..\dbt"

echo.
echo Installing dbt packages...
dbt deps

echo.
echo ==========================================
echo Installation complete!
echo.
echo Next steps:
echo 1. Configure profiles.yml with your connection details
echo 2. Run: dbt deps
echo 3. Run: dbt run --select semantic
echo 4. Run: dbt test
echo 5. Run: dbt docs generate ^&^& dbt docs serve
echo ==========================================

pause
