@echo off
title Real-Time Customer Segmentation - DEPLOYED MODE

REM Set environment variable for deploy mode
set MODE=deploy

REM Check if Python is available
where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python is not found in the system PATH. Please install Python and add it to PATH.
    pause
    exit /b 1
)

REM Set up the database tables with error logging
echo Starting database setup...
python setup_database.py > setup_log.txt 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Database setup failed. Check setup_log.txt for details.
    type setup_log.txt
    pause
    exit /b 1
)
echo Database setup completed successfully.

REM Start ZooKeeper with error logging
start "ZooKeeper" cmd /k "cd /d C:\kafka && echo Starting ZooKeeper... > zookeeper_log.txt 2>&1 && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties >> zookeeper_log.txt 2>&1"

REM Start Kafka with error logging
start "Kafka" cmd /k "cd /d C:\kafka && echo Starting Kafka... > kafka_log.txt 2>&1 && .\bin\windows\kafka-server-start.bat .\config\server.properties >> kafka_log.txt 2>&1"

REM Increased wait time to ensure services are fully started
timeout /t 30

REM Create Kafka topic with error logging
cd /d C:\kafka\bin\windows
echo Creating topic customer-data...
kafka-topics.bat --create --topic customer-data --bootstrap-server localhost:9092 > topic_log.txt 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to create topic. Check topic_log.txt for details.
    type topic_log.txt
    pause
    exit /b 1
)
echo Topic customer-data created successfully.

REM Start Producer
start "Producer" cmd /k "set MODE=deploy && python producer.py"

REM Start Consumer
start "Consumer" cmd /k "set MODE=deploy && python consumer.py"

REM Start Dashboard
start "Dashboard" cmd /k "set MODE=deploy && streamlit run dashboard.py"

echo.
echo ✅ All services started in DEPLOYED mode!
echo Logs are available in setup_log.txt, zookeeper_log.txt, kafka_log.txt, and topic_log.txt.
echo Press any key to keep this window open...
pause