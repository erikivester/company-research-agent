#!/bin/bash

# Complete launcher for Airtable extension with API server and ngrok
# This script starts everything needed for the extension to work

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "🚀 Airtable Extension Complete Launcher"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check prerequisites
echo "✓ Checking prerequisites..."

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

# Check npm
if ! command -v npm &> /dev/null; then
    echo "❌ npm is required but not installed"
    exit 1
fi

# Check ngrok
if ! command -v ngrok &> /dev/null; then
    echo "❌ ngrok is required but not installed"
    echo "   Install it from: https://ngrok.com/download"
    exit 1
fi

# Check Airtable CLI
if ! command -v block &> /dev/null; then
    echo "❌ Airtable Blocks CLI is required but not installed"
    echo "   Install it with: npm install -g @airtable/blocks-cli"
    exit 1
fi

echo "✅ All prerequisites installed"
echo ""

# Setup Python environment
if [ ! -d ".venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
    echo "✅ Virtual environment created"
else
    source .venv/bin/activate
fi

# Check .env file
if [ ! -f ".env" ]; then
    echo "⚠️  Warning: .env file not found. Server may have missing configuration."
fi

# Setup Node environment for extension
cd scripting
if [ ! -d "node_modules" ]; then
    echo "📦 Installing extension dependencies..."
    npm install
    echo "✅ Dependencies installed"
fi
cd ..

PORT=${PORT:-8000}

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "🔧 Starting Services..."
echo "════════════════════════════════════════════════════════════════"
echo ""

# Start FastAPI server
echo "1️⃣  Starting FastAPI server on port $PORT..."
uvicorn application:app --host 0.0.0.0 --port $PORT > server.log 2>&1 &
UVICORN_PID=$!

sleep 3

if ! kill -0 $UVICORN_PID 2>/dev/null; then
    echo "❌ Failed to start server. Check server.log for details."
    exit 1
fi

echo "   ✅ Server running (PID: $UVICORN_PID)"

# Start ngrok
echo ""
echo "2️⃣  Starting ngrok tunnel..."
ngrok http $PORT --log=stdout > ngrok.log 2>&1 &
NGROK_PID=$!

sleep 4

# Get ngrok URL
NGROK_URL=""
for i in {1..10}; do
    NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    if [ ! -z "$NGROK_URL" ]; then
        break
    fi
    sleep 2
done

if [ -z "$NGROK_URL" ]; then
    echo "   ❌ Failed to get ngrok URL. Check ngrok.log"
    kill $UVICORN_PID 2>/dev/null || true
    kill $NGROK_PID 2>/dev/null || true
    exit 1
fi

echo "   ✅ Tunnel established: $NGROK_URL"

# Test API endpoint
echo ""
echo "3️⃣  Testing API connection..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "ngrok-skip-browser-warning: true" "$NGROK_URL/templates" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" == "200" ] || [ "$HTTP_CODE" == "401" ]; then
    echo "   ✅ API is responding"
else
    echo "   ⚠️  Warning: API returned HTTP $HTTP_CODE (may need authentication)"
fi

# Start Airtable extension
echo ""
echo "4️⃣  Starting Airtable extension..."
echo ""

cd scripting

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down all services..."
    kill $UVICORN_PID 2>/dev/null || true
    kill $NGROK_PID 2>/dev/null || true
    echo "✅ All services stopped"
    exit 0
}

trap cleanup INT TERM

echo "════════════════════════════════════════════════════════════════"
echo "✅ ALL SYSTEMS READY!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📡 Ngrok URL: $NGROK_URL"
echo "🏠 Local API: http://localhost:$PORT"
echo "📊 Ngrok Dashboard: http://localhost:4040"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📋 CONFIGURATION STEPS:"
echo ""
echo "   The extension will open in your browser..."
echo ""
echo "   1. Click Settings (⚙️) in the extension"
echo ""
echo "   2. Paste this URL in 'API Endpoint':"
echo "      $NGROK_URL"
echo ""
echo "   3. Select your table and map fields"
echo ""
echo "   4. Click 'Save' and start generating emails!"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📁 Logs:"
echo "   - API Server: $PROJECT_DIR/server.log"
echo "   - Ngrok: $PROJECT_DIR/ngrok.log"
echo ""
echo "🛑 Press Ctrl+C to stop all services"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

# Run the extension (this will open browser)
block run --port 9000

# If block run exits, cleanup
cleanup
