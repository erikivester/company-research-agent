#!/bin/bash

# Simple launcher for Airtable extension with ngrok
# This exposes your local extension so Airtable can access it

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "🚀 Airtable Extension with Ngrok"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Shutting down..."
    pkill -f "block run" || true
    pkill -f "ngrok http" || true
    echo "✅ All services stopped."
    exit 0
}

trap cleanup INT TERM

# Kill any existing processes
echo "🧹 Cleaning up existing processes..."
pkill -f "block run" || true
pkill -f "ngrok http" || true
sleep 2
echo "✅ Cleanup complete."
echo ""

# Check prerequisites
if ! command -v block &> /dev/null; then
    echo "❌ Airtable Blocks CLI not installed"
    echo "   Install: npm install -g @airtable/blocks-cli"
    exit 1
fi

if ! command -v ngrok &> /dev/null; then
    echo "❌ ngrok not installed"
    echo "   Install from: https://ngrok.com/download"
    exit 1
fi

echo "✅ Prerequisites installed"
echo ""

# Start the extension
echo "1️⃣  Starting Airtable extension on port 9000..."
cd scripting
block run --port 9000 > ../extension.log 2>&1 &
cd ..
sleep 8

if ! pgrep -f "block run" > /dev/null; then
    echo "❌ Failed to start extension. Check extension.log"
    exit 1
fi
echo "   ✅ Extension running on https://localhost:9000"
echo ""

# Start ngrok
echo "2️⃣  Starting ngrok tunnel..."
ngrok http https://localhost:9000 --log=stdout > ngrok.log 2>&1 &
sleep 5

# Get the ngrok URL
echo "3️⃣  Getting public URL..."
NGROK_URL=""
for i in {1..10}; do
    NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | head -n 1 | sed 's/"public_url":"//') 
    if [ -n "$NGROK_URL" ]; then
        break
    fi
    sleep 2
done

if [ -z "$NGROK_URL" ]; then
    echo "   ❌ Failed to get ngrok URL. Check ngrok.log"
    cleanup
    exit 1
fi

echo "   ✅ Tunnel established!"
echo ""

echo "════════════════════════════════════════════════════════════════"
echo "✅ EXTENSION IS READY!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📋 COPY THIS URL AND PASTE IT INTO AIRTABLE:"
echo ""
echo "   $NGROK_URL"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📍 Steps to add the extension:"
echo "   1. Open your Airtable base"
echo "   2. Click 'Extensions' (puzzle icon)"
echo "   3. Click 'Add an extension'"
echo "   4. Select 'Build a custom extension'"
echo "   5. Paste the URL above when prompted"
echo ""
echo "🌐 Ngrok Dashboard: http://localhost:4040"
echo ""
echo "🛑 Press Ctrl+C to stop all services"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

# Keep running
wait
