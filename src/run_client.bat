@echo off
title Stock Market CLIENT
echo Compiling...
if not exist bin mkdir bin
javac -d bin StockTradingPlatform.java
if %errorlevel% neq 0 exit /b %errorlevel%

set /p username="Enter your username: "
title Stock Market CLIENT - %username%
java -cp bin StockTradingPlatform client %username%
pause