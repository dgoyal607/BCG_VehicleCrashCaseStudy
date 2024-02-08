@echo off
setlocal

rem Remove existing VehicleCrash directory if it exists
if exist VehicleCrash rmdir /s /q VehicleCrash

rem Create VehicleCrash directory
mkdir VehicleCrash

rem Copy main.py to VehicleCrash directory
copy main.py VehicleCrash

rem Copy configs directory to VehicleCrash directory
xcopy /E /I configs VehicleCrash\configs

rem Copy utils directory to VehicleCrash directory
xcopy /E /I utils VehicleCrash\utils

rem Copy Data directory to VehicleCrash directory
xcopy /E /I Data VehicleCrash\Data

rem Create a ZIP archive
powershell Compress-Archive -Path .\VehicleCrash\* -DestinationPath VehicleCrash.zip -Force


endlocal
