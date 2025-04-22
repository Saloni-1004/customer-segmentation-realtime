@echo off
title Real-Time Customer Segmentation - LOCAL MODE

REM Set environment variable for local mode
set MODE=local

REM Check if Python is available
where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python is not found in the system PATH. Please install Python and add it to PATH.
    pause
    exit /b 1
)

REM Check if PostgreSQL is running
echo Checking if PostgreSQL is running...
python -c "import socket; s=socket.socket(); result=s.connect_ex(('localhost', 5432)); s.close(); exit(0 if result == 0 else 1)" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: PostgreSQL is not running on localhost:5432.
    echo Please start PostgreSQL server and try again.
    pause
    exit /b 1
)
echo PostgreSQL is running.

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

REM Check if Kafka directory exists
if not exist C:\kafka (
    echo ERROR: Kafka directory not found at C:\kafka.
    echo Please install Kafka and update the path in this script.
    pause
    exit /b 1
)

REM Kill any existing ZooKeeper or Kafka processes
echo Stopping any existing ZooKeeper or Kafka processes...
taskkill /F /FI "WINDOWTITLE eq ZooKeeper*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Kafka*" >nul 2>&1

REM Start ZooKeeper with error logging
start "ZooKeeper" cmd /k "cd /d C:\kafka && echo Starting ZooKeeper... > zookeeper_log.txt 2>&1 && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties >> zookeeper_log.txt 2>&1"

REM Wait for ZooKeeper to start
echo Waiting for ZooKeeper to start...
timeout /t 15 > nul

REM Start Kafka with error logging
start "Kafka" cmd /k "cd /d C:\kafka && echo Starting Kafka... > kafka_log.txt 2>&1 && .\bin\windows\kafka-server-start.bat .\config\server.properties >> kafka_log.txt 2>&1"

REM Wait for Kafka to start
echo Waiting for Kafka to start...
timeout /t 30 > nul

REM Check if Kafka is running by trying to list topics
cd /d C:\kafka\bin\windows
echo Checking if Kafka is running...
kafka-topics.bat --list --bootstrap-server localhost:9092 > kafka_check.txt 2>&1
find "customer-data" kafka_check.txt >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo Topic customer-data already exists.
) else (
    echo Creating topic customer-data...
    kafka-topics.bat --create --topic customer-data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 > topic_log.txt 2>&1
    if %ERRORLEVEL% neq 0 (
        echo ERROR: Failed to create topic. Check topic_log.txt for details.
        type topic_log.txt
        pause
        exit /b 1
    )
    echo Topic customer-data created successfully.
)

REM Start Producer
start "Producer" cmd /k "set MODE=local && python producer.py"

REM Start Consumer
start "Consumer" cmd /k "set MODE=local && python consumer.py"

REM Start Dashboard
start "Dashboard" cmd /k "set MODE=local && streamlit run dashboard.py"

echo.
echo ✅ All services started in LOCAL mode!
echo Logs are available in setup_log.txt, zookeeper_log.txt, kafka_log.txt, and topic_log.txt.
echo Press any key to keep this window open...
pause