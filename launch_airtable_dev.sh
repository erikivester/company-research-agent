#!/bin/bash

# Launcher for Airtable extension with dual ngrok tunnels
# This script starts the backend API and the extension frontend, each with its own ngrok tunnel.

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "🚀 Airtable Extension Dual Ngrok Launcher"
echo "════════════════════════════════════════════════════════════════"
echo ""

# --- Prerequisite Checks ---
echo "🔎 Checking prerequisites..."
command -v python3 >/dev/null 2>&1 || { echo "❌ Python 3 is required but not installed."; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "❌ npm is required but not installed."; exit 1; }
command -v ngrok >/dev/null 2>&1 || { echo "❌ ngrok is required. Install from https://ngrok.com/download"; exit 1; }
command -v block >/dev/null 2>&1 || { echo "❌ Airtable CLI is required. Run: npm install -g @airtable/blocks-cli"; exit 1; }
echo "✅ All prerequisites are installed."
echo ""

# --- Cleanup Function ---
cleanup() {
    echo ""
    echo "🛑 Shutting down all services..."
    pkill -f "uvicorn application:app" || true
    pkill -f "ngrok http" || true
    pkill -f "block run" || true
    echo "✅ All services stopped."
    # Exit only if called from the trap
    if [ "$1" = "trap" ]; then
        exit 0
    fi
}

trap "cleanup trap" INT TERM

# --- Source Environment Variables ---
if [ -f .env ]; then
    echo "🔎 Loading environment variables from .env file..."
    set -o allexport
    source .env
    set +o allexport
    echo "✅ Environment variables loaded."
    echo ""
fi

# --- Kill Existing Processes ---
echo "🧹 Cleaning up any running processes..."
cleanup # Perform initial cleanup without exiting
sleep 2 # Give processes time to shut down
echo "✅ Cleanup complete."
echo ""

# --- Setup Python Environment ---
if [ ! -d ".venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
    echo "✅ Virtual environment created."
else
    source .venv/bin/activate
fi

# --- Setup Node Environment ---
cd scripting
if [ ! -d "node_modules" ]; then
    echo "📦 Installing extension dependencies..."
    npm install --silent
    echo "✅ Dependencies installed."
fi
cd ..

# --- Define Ports ---
API_PORT=8000
EXTENSION_PORT=9000
NGROK_API_PORT=4040
NGROK_LOG="ngrok.log"

echo "════════════════════════════════════════════════════════════════"
echo "🔧 Starting Services..."
echo "════════════════════════════════════════════════════════════════"
echo ""

# --- 1. Start FastAPI Backend ---
echo "1️⃣  Starting FastAPI backend on port $API_PORT..."
uvicorn application:app --host 0.0.0.0 --port $API_PORT > backend.log 2>&1 &
sleep 3
if ! pgrep -f "uvicorn application:app" > /dev/null; then
    echo "❌ Failed to start backend server. Check backend.log for details."
    exit 1
fi
echo "   ✅ Backend server is running."
echo ""

# --- 2. Start Airtable Extension Frontend ---
echo "2️⃣  Starting Airtable extension on port $EXTENSION_PORT..."
cd scripting
block run --port $EXTENSION_PORT > ../frontend.log 2>&1 &
cd ..
sleep 5 # Give block server time to start
if ! pgrep -f "block run" > /dev/null; then
    echo "❌ Failed to start Airtable extension. Check frontend.log for details."
    cleanup
    exit 1
fi
echo "   ✅ Airtable extension server is running."
echo ""

# --- 3. Start Ngrok Tunnels from Config ---
echo "3️⃣  Starting ngrok tunnel for Airtable extension..."
if [ -z "$NGROK_AUTHTOKEN" ]; then
    echo "⚠️  Warning: NGROK_AUTHTOKEN environment variable is not set."
    echo "   Please get your token from https://dashboard.ngrok.com/get-started/your-authtoken"
    echo "   And run: export NGROK_AUTHTOKEN=\"YOUR_TOKEN\""
fi
# Only tunnel the frontend - backend can be accessed via localhost within the extension
ngrok http 9000 --log=stdout > "$NGROK_LOG" 2>&1 &
sleep 8 # Allow time for tunnel to establish

# --- 4. Retrieve Ngrok URLs ---
echo "4️⃣  Fetching ngrok URL..."
FRONTEND_URL=""
for i in {1..15}; do # Increased retries
    TUNNELS_JSON=$(curl -s http://localhost:$NGROK_API_PORT/api/tunnels)
    
    # Check if jq is installed
    if command -v jq >/dev/null 2>&1; then
        FRONTEND_URL=$(echo $TUNNELS_JSON | jq -r '.tunnels[0].public_url' | grep -v null)
    else
        # Fallback to grep if jq is not available
        FRONTEND_URL=$(echo $TUNNELS_JSON | grep -o '"public_url":"https://[^"]*' | head -n 1 | sed 's/"public_url":"//')
    fi

    if [ -n "$FRONTEND_URL" ] && [ "$FRONTEND_URL" != "null" ]; then
        break
    fi
    sleep 2
done

if [ -z "$FRONTEND_URL" ] || [ "$FRONTEND_URL" == "null" ]; then
    echo "   ❌ Failed to get ngrok URL. Check $NGROK_LOG for details."
    cleanup trap
    exit 1
fi
echo "   ✅ URL fetched successfully."
echo ""


echo "════════════════════════════════════════════════════════════════"
echo "✅ ALL SYSTEMS READY!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📋 INSTRUCTIONS FOR AIRTABLE:"
echo ""
echo "1. In Airtable, when prompted for the extension URL, use this:"
echo "   ➡️  $FRONTEND_URL"
echo ""
echo "2. Inside your extension's settings, for the 'API Endpoint', use:"
echo "   ➡️  http://localhost:$API_PORT"
echo "   (The extension runs in a local browser and can access localhost)"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "   - Backend API running at: http://localhost:$API_PORT"
echo "   - Extension running at: http://localhost:$EXTENSION_PORT"
echo "   - Extension public URL: $FRONTEND_URL"
echo "   - Ngrok Dashboard: http://localhost:$NGROK_API_PORT"
echo ""
echo "🛑 Press Ctrl+C to shut down all services."
echo ""

# Wait for user to exit
wait
