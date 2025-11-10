#!/bin/bash

# Complete launcher with DOUBLE NGROK tunnels
# For university WiFi or any network that blocks localhost
# This creates ngrok tunnels for BOTH API and Extension

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "🎓 Airtable Extension + API (Double Ngrok for University WiFi)"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check prerequisites
echo "✓ Checking prerequisites..."

if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "❌ npm is required"
    exit 1
fi

if ! command -v ngrok &> /dev/null; then
    echo "❌ ngrok is required. Install from: https://ngrok.com/download"
    exit 1
fi

if ! command -v block &> /dev/null; then
    echo "❌ Airtable Blocks CLI required. Install: npm install -g @airtable/blocks-cli"
    exit 1
fi

echo "✅ All prerequisites installed"
echo ""

# Setup environments
if [ ! -d ".venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
else
    source .venv/bin/activate
fi

cd scripting
if [ ! -d "node_modules" ]; then
    echo "📦 Installing extension dependencies..."
    npm install
fi
cd ..

API_PORT=8000
EXT_PORT=9002

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "🚀 Starting All Services with Ngrok Tunnels..."
echo "════════════════════════════════════════════════════════════════"
echo ""

# 1. Start API server
echo "1️⃣  Starting API server on port $API_PORT..."
uvicorn application:app --host 0.0.0.0 --port $API_PORT > api-server.log 2>&1 &
API_PID=$!
sleep 3

if ! kill -0 $API_PID 2>/dev/null; then
    echo "❌ Failed to start API server. Check api-server.log"
    exit 1
fi
echo "   ✅ API server running (PID: $API_PID)"

# 2. Start ngrok for API
echo ""
echo "2️⃣  Creating ngrok tunnel for API..."
ngrok http $API_PORT --log=stdout > ngrok-api.log 2>&1 &
NGROK_API_PID=$!
sleep 4

API_URL=""
for i in {1..10}; do
    API_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    if [ ! -z "$API_URL" ]; then
        break
    fi
    sleep 2
done

if [ -z "$API_URL" ]; then
    echo "   ❌ Failed to get API ngrok URL"
    kill $API_PID 2>/dev/null || true
    kill $NGROK_API_PID 2>/dev/null || true
    exit 1
fi
echo "   ✅ API tunnel: $API_URL"

# 3. Start Extension server
echo ""
echo "3️⃣  Starting Airtable extension on port $EXT_PORT..."
cd scripting
block run --port $EXT_PORT > ../extension-server.log 2>&1 &
EXT_PID=$!
cd ..
sleep 4

if ! kill -0 $EXT_PID 2>/dev/null; then
    echo "   ❌ Failed to start extension. Check extension-server.log"
    kill $API_PID 2>/dev/null || true
    kill $NGROK_API_PID 2>/dev/null || true
    exit 1
fi
echo "   ✅ Extension server running (PID: $EXT_PID)"

# 4. Start ngrok for Extension on different port (since 4040 is used)
echo ""
echo "4️⃣  Creating ngrok tunnel for Extension..."
ngrok http https://localhost:$EXT_PORT --domain="" --log=stdout > ngrok-extension.log 2>&1 &
NGROK_EXT_PID=$!
sleep 4

# Get extension URL from second ngrok (it will use port 4041)
EXT_URL=""
for i in {1..10}; do
    EXT_URL=$(curl -s http://localhost:4041/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    if [ ! -z "$EXT_URL" ]; then
        break
    fi
    sleep 2
done

if [ -z "$EXT_URL" ]; then
    echo "   ⚠️  Warning: Could not get extension ngrok URL automatically"
    echo "   Check http://localhost:4041 for the URL"
else
    echo "   ✅ Extension tunnel: $EXT_URL"
fi

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Shutting down all services..."
    kill $API_PID 2>/dev/null || true
    kill $NGROK_API_PID 2>/dev/null || true
    kill $EXT_PID 2>/dev/null || true
    kill $NGROK_EXT_PID 2>/dev/null || true
    echo "✅ All services stopped"
    exit 0
}

trap cleanup INT TERM

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ ALL SYSTEMS READY - UNIVERSITY WIFI COMPATIBLE!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "🌐 Ngrok Tunnels (use these, NOT localhost):"
echo ""
echo "   📡 API Server:"
echo "      $API_URL"
echo ""
if [ ! -z "$EXT_URL" ]; then
echo "   🔧 Extension:"
echo "      $EXT_URL"
else
echo "   🔧 Extension: Check http://localhost:4041"
fi
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📋 CONFIGURATION STEPS:"
echo ""
echo "   1. Open Airtable and go to your base"
echo ""
echo "   2. Click 'Extensions' → 'Add an extension' → 'Build custom extension'"
echo ""
if [ ! -z "$EXT_URL" ]; then
echo "   3. Paste THIS URL (Extension ngrok URL):"
echo "      $EXT_URL"
else
echo "   3. Go to http://localhost:4041 and copy the HTTPS URL"
echo "      Paste it in Airtable"
fi
echo ""
echo "   4. Once extension loads, click Settings (⚙️)"
echo ""
echo "   5. Set API Endpoint to (API ngrok URL):"
echo "      $API_URL"
echo ""
echo "   6. Map your fields and start generating emails!"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Monitoring:"
echo "   - API Ngrok Dashboard: http://localhost:4040"
echo "   - Extension Ngrok Dashboard: http://localhost:4041"
echo "   - API Logs: $PROJECT_DIR/api-server.log"
echo "   - Extension Logs: $PROJECT_DIR/extension-server.log"
echo ""
echo "🛑 Press Ctrl+C to stop all services"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

# Keep script running
wait
