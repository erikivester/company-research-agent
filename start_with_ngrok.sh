#!/bin/bash

# Start FastAPI server with ngrok for Airtable extension
# This script starts the uvicorn server and creates an ngrok tunnel

set -e

echo "🚀 Starting Company Research API with ngrok tunnel..."
echo ""

# Check if .venv exists
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment not found. Creating one..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
else
    source .venv/bin/activate
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "⚠️  Warning: .env file not found. Make sure environment variables are set."
fi

# Find available port (default 8000)
PORT=${PORT:-8000}

echo "📦 Starting uvicorn server on port $PORT..."
# Start uvicorn in background
uvicorn application:app --host 0.0.0.0 --port $PORT --reload &
UVICORN_PID=$!

# Wait for server to start
echo "⏳ Waiting for server to start..."
sleep 3

# Check if server is running
if ! kill -0 $UVICORN_PID 2>/dev/null; then
    echo "❌ Failed to start uvicorn server"
    exit 1
fi

echo "✅ Server started (PID: $UVICORN_PID)"
echo ""

# Start ngrok tunnel
echo "🌐 Starting ngrok tunnel..."
ngrok http $PORT --log=stdout > ngrok.log 2>&1 &
NGROK_PID=$!

# Wait for ngrok to start
echo "⏳ Waiting for ngrok to establish tunnel..."
sleep 4

# Get ngrok URL
NGROK_URL=""
for i in {1..10}; do
    NGROK_URL=$(curl -s http://localhost:4040/api/tunnels | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    if [ ! -z "$NGROK_URL" ]; then
        break
    fi
    echo "   Attempt $i/10..."
    sleep 2
done

if [ -z "$NGROK_URL" ]; then
    echo "❌ Failed to get ngrok URL"
    echo "   Check ngrok.log for details"
    kill $UVICORN_PID 2>/dev/null || true
    kill $NGROK_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ SERVER READY!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📡 API Endpoint: $NGROK_URL"
echo "🔧 Local Server: http://localhost:$PORT"
echo "📊 Ngrok Dashboard: http://localhost:4040"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📋 NEXT STEPS FOR AIRTABLE EXTENSION:"
echo ""
echo "   1. In a NEW terminal, run:"
echo "      cd scripting"
echo "      block run"
echo ""
echo "   2. In the Airtable extension, click Settings (⚙️)"
echo ""
echo "   3. Set API Endpoint to:"
echo "      $NGROK_URL"
echo ""
echo "   4. Configure your field mappings"
echo ""
echo "   5. Start generating emails!"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📝 Logs:"
echo "   - Server logs: stdout"
echo "   - Ngrok logs: ngrok.log"
echo ""
echo "🛑 To stop: Press Ctrl+C"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Shutting down..."
    kill $UVICORN_PID 2>/dev/null || true
    kill $NGROK_PID 2>/dev/null || true
    echo "✅ Stopped"
    exit 0
}

trap cleanup INT TERM

# Keep script running
wait $UVICORN_PID
