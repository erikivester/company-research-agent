http://localhost:8000#!/bin/bash

# Unified launcher with TWO ngrok tunnels:
# 1. Unified tunnel through nginx (for production use)
# 2. Direct extension tunnel (for Airtable preview/development)

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "🚀 Unified Launch: AI Research Agent + Airtable Extension"
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

if ! command -v nginx &> /dev/null; then
    echo "❌ nginx is required. Install with: brew install nginx"
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
else
    source .venv/bin/activate
fi

# Setup Node environment for extension
cd email
if [ ! -d "node_modules" ]; then
    echo "📦 Installing extension dependencies..."
    npm install
fi
cd ..

API_PORT=8000
EXT_PORT=9002
NGINX_PORT=8080

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "🚀 Starting All Services..."
echo "════════════════════════════════════════════════════════════════"
echo ""

# Kill any existing processes from previous runs
echo "🧹 Cleaning up any existing processes..."
pkill -f "uvicorn application:app" 2>/dev/null || true
pkill -f "block run --port $EXT_PORT" 2>/dev/null || true
pkill nginx 2>/dev/null || true
pkill ngrok 2>/dev/null || true
sleep 2
echo "   ✅ Cleanup complete"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Shutting down all services..."
    kill $API_PID 2>/dev/null || true
    kill $EXT_PID 2>/dev/null || true
    pkill nginx 2>/dev/null || true
    kill $NGROK_UNIFIED_PID 2>/dev/null || true
    kill $NGROK_EXT_PID 2>/dev/null || true
    echo "✅ All services stopped"
    exit 0
}

trap cleanup INT TERM

# 1. Start API server
echo "1️⃣  Starting API server on port $API_PORT..."
uvicorn application:app --host 0.0.0.0 --port $API_PORT --log-level warning > api-server.log 2>&1 &
API_PID=$!
sleep 3

if ! kill -0 $API_PID 2>/dev/null; then
    echo "❌ Failed to start API server. Check api-server.log"
    exit 1
fi
echo "   ✅ API server running (PID: $API_PID)"

# 2. Start Extension server
echo ""
echo "2️⃣  Starting Airtable extension on port $EXT_PORT..."
cd email
block run --port $EXT_PORT > ../extension-server.log 2>&1 &
EXT_PID=$!
cd ..
sleep 4

if ! kill -0 $EXT_PID 2>/dev/null; then
    echo "   ❌ Failed to start extension. Check extension-server.log"
    cleanup
fi
echo "   ✅ Extension server running (PID: $EXT_PID)"

# 3. Start nginx reverse proxy
echo ""
echo "3️⃣  Starting nginx reverse proxy on port $NGINX_PORT..."

# Kill any existing nginx first
pkill nginx 2>/dev/null || true
sleep 1

# Test config first
if ! nginx -t -c "$PROJECT_DIR/nginx.conf" -p "$PROJECT_DIR" > /dev/null 2>&1; then
    echo "   ❌ nginx configuration test failed"
    nginx -t -c "$PROJECT_DIR/nginx.conf" -p "$PROJECT_DIR"
    cleanup
fi

# Start nginx
nginx -c "$PROJECT_DIR/nginx.conf" -p "$PROJECT_DIR" 2>&1 | tee nginx.log &
sleep 2

# Check if nginx started successfully
if ! lsof -i :$NGINX_PORT > /dev/null 2>&1; then
    echo "   ❌ Failed to start nginx. Error:"
    cat nginx.log
    cleanup
fi
echo "   ✅ Nginx running"

# 4. Start unified ngrok tunnel (for production use through nginx)
echo ""
echo "4️⃣  Creating unified ngrok tunnel (through nginx)..."
ngrok http $NGINX_PORT --log=stdout > ngrok-unified.log 2>&1 &
NGROK_UNIFIED_PID=$!
sleep 4

NGROK_UNIFIED_URL=""
for i in {1..10}; do
    NGROK_UNIFIED_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    if [ ! -z "$NGROK_UNIFIED_URL" ]; then
        break
    fi
    sleep 2
done

if [ -z "$NGROK_UNIFIED_URL" ]; then
    echo "   ❌ Failed to get unified ngrok URL"
    cleanup
fi
echo "   ✅ Unified tunnel: $NGROK_UNIFIED_URL"

# 5. Start extension ngrok tunnel (for Airtable preview)
echo ""
echo "5️⃣  Creating extension preview tunnel (direct to port $EXT_PORT)..."
ngrok http https://localhost:$EXT_PORT --log=stdout > ngrok-extension.log 2>&1 &
NGROK_EXT_PID=$!
sleep 4

NGROK_EXT_URL=""
for i in {1..10}; do
    NGROK_EXT_URL=$(curl -s http://localhost:4041/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    if [ ! -z "$NGROK_EXT_URL" ]; then
        break
    fi
    sleep 2
done

if [ -z "$NGROK_EXT_URL" ]; then
    echo "   ⚠️  Warning: Could not get extension preview URL"
    echo "   Check http://localhost:4041 for the URL"
    NGROK_EXT_URL="(Check http://localhost:4041)"
else
    echo "   ✅ Extension preview: $NGROK_EXT_URL"
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ ALL SYSTEMS READY!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "🌐 NGROK URLs:"
echo ""
echo "   📧 FOR AIRTABLE EXTENSION PREVIEW (paste in Airtable):"
echo "      $NGROK_EXT_URL"
echo ""
echo "   🔗 FOR API ENDPOINT (paste in extension Settings):"
echo "      $NGROK_UNIFIED_URL"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📋 CONFIGURATION STEPS FOR AIRTABLE:"
echo ""
echo "   1. Open Airtable and go to your base"
echo ""
echo "   2. Click 'Extensions' → 'Add an extension' → 'Build custom extension'"
echo ""
echo "   3. Paste the EXTENSION PREVIEW URL:"
echo "      $NGROK_EXT_URL"
echo ""
echo "   4. Once extension loads, click Settings (⚙️)"
echo ""
echo "   5. Set API Endpoint to the UNIFIED URL:"
echo "      $NGROK_UNIFIED_URL"
echo ""
echo "   6. Map your fields and start generating emails!"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Service Access:"
echo "   - API: http://localhost:$API_PORT"
echo "   - Extension: https://localhost:$EXT_PORT"
echo "   - Nginx Proxy: http://localhost:$NGINX_PORT"
echo "   - Unified Ngrok Dashboard: http://localhost:4040"
echo "   - Extension Ngrok Dashboard: http://localhost:4041"
echo ""
echo "📡 How the routing works:"
echo "   - Extension UI → Direct tunnel → Extension (port $EXT_PORT)"
echo "   - API calls → Unified tunnel → nginx → API (port $API_PORT)"
echo ""
echo "📁 Logs:"
echo "   - API: $PROJECT_DIR/api-server.log"
echo "   - Extension: $PROJECT_DIR/extension-server.log"
echo "   - Nginx: $PROJECT_DIR/nginx.log"
echo "   - Unified Ngrok: $PROJECT_DIR/ngrok-unified.log"
echo "   - Extension Ngrok: $PROJECT_DIR/ngrok-extension.log"
echo ""
echo "🛑 Press Ctrl+C to stop all services"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

# Keep script running
wait
