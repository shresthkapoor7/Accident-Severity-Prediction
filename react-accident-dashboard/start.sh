#!/bin/bash

echo "🚗 US Accidents Dashboard - Starting..."

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down servers..."
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start Backend
echo "📡 Starting Flask Backend (port 5000)..."
cd backend
python app.py &
BACKEND_PID=$!
cd ..

# Wait for backend to initialize
echo "⏳ Waiting for backend to initialize..."
sleep 10

# Start Frontend
echo "⚛️ Starting React Frontend (port 3000)..."
cd frontend
npm start &
FRONTEND_PID=$!
cd ..

echo ""
echo "✅ Dashboard is running!"
echo "   Frontend: http://localhost:3000"
echo "   Backend:  http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop both servers"

# Wait for processes
wait
