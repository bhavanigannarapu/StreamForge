@echo off
echo ===================================================
echo 🚀 Launching StreamForge Enterprise System
echo ===================================================

echo 1. Starting Telemetry Producer...
start "1. Producer" cmd /k "python producer/producer.py"

echo 2. Starting Stream Worker...
start "2. Worker" cmd /k "python workers/worker.py"

echo 3. Starting State Manager...
start "3. State Manager" cmd /k "python state/state_manager.py"

echo 4. Starting Alert Engine...
start "4. Alert Engine" cmd /k "python alert_engine.py"

echo 5. Starting Backend API...
start "5. Backend API" cmd /k "python run_api.py"

echo 6. Launching Streamlit Operational Dashboard...
start "6. Streamlit Dashboard" cmd /k "python -m streamlit run dashboard/app.py"

echo ===================================================
echo ✅ All microservices initiated successfully!
echo ===================================================