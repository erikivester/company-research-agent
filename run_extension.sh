#!/bin/bash

# Simple script to run the Airtable extension in development mode
# This script starts ONLY the extension - backend API runs separately if needed

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "🚀 Airtable Extension Development Mode"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check if block CLI is installed
if ! command -v block &> /dev/null; then
    echo "❌ Airtable Blocks CLI is not installed."
    echo "   Install it with: npm install -g @airtable/blocks-cli"
    exit 1
fi

# Check if we're in the scripting directory or need to cd into it
if [ ! -f "block.json" ]; then
    if [ -f "scripting/block.json" ]; then
        cd scripting
        echo "✅ Found extension in scripting directory"
    else
        echo "❌ Cannot find block.json. Are you in the right directory?"
        exit 1
    fi
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
    echo "✅ Dependencies installed"
    echo ""
fi

echo "════════════════════════════════════════════════════════════════"
echo "📋 IMPORTANT INSTRUCTIONS:"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "1. Make sure you're logged in to Airtable CLI:"
echo "   Run: block init"
echo "   Or: block set"
echo ""
echo "2. This will open your browser and start the extension"
echo ""
echo "3. In Airtable:"
echo "   - Open your base"
echo "   - Click 'Extensions' (puzzle icon)"
echo "   - Click 'Add an extension'"
echo "   - Choose 'Build a custom extension'"
echo "   - When prompted, the extension should auto-detect"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Starting extension in development mode..."
echo ""

# Run the block
block run --port 9000

