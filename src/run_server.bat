@echo off
title Stock Market SERVER
echo Compiling...
if not exist bin mkdir bin
javac -d bin StockTradingPlatform.java
if %errorlevel% neq 0 exit /b %errorlevel%

echo Starting Server...
java -cp bin StockTradingPlatform server
pause