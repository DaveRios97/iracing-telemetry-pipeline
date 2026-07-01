@echo off
echo ============================================
echo  Building iRacing Telemetry Collector .exe
echo ============================================

echo Limpiando builds anteriores...
rmdir /s /q build 2>nul
rmdir /s /q dist 2>nul
del *.spec 2>nul

echo Instalando PyInstaller (por si acaso)...
pip install pyinstaller

echo.
echo Compilando ejecutable...
pyinstaller --onefile ^
    --name iRacingCollector ^
    main_collector.py

echo.
echo ============================================
echo  Build completado!
echo  El ejecutable esta en la carpeta: dist\iRacingCollector.exe
echo ============================================
pause
