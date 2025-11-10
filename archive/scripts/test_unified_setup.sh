#!/bin/bash

# Test script to verify unified setup prerequisites

echo "════════════════════════════════════════════════════════════════"
echo "🧪 Testing Unified NGROK Setup Prerequisites"
echo "════════════════════════════════════════════════════════════════"
echo ""

ERRORS=0

# Check Python
echo -n "Testing Python 3... "
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    echo "✅ Found ($PYTHON_VERSION)"
else
    echo "❌ Not found"
    ERRORS=$((ERRORS+1))
fi

# Check npm
echo -n "Testing npm... "
if command -v npm &> /dev/null; then
    NPM_VERSION=$(npm --version 2>&1)
    echo "✅ Found ($NPM_VERSION)"
else
    echo "❌ Not found"
    ERRORS=$((ERRORS+1))
fi

# Check ngrok
echo -n "Testing ngrok... "
if command -v ngrok &> /dev/null; then
    NGROK_VERSION=$(ngrok version 2>&1 | head -n1)
    echo "✅ Found ($NGROK_VERSION)"
else
    echo "❌ Not found"
    echo "   Install: brew install ngrok"
    ERRORS=$((ERRORS+1))
fi

# Check Airtable CLI
echo -n "Testing Airtable CLI... "
if command -v block &> /dev/null; then
    echo "✅ Found"
else
    echo "❌ Not found"
    echo "   Install: npm install -g @airtable/blocks-cli"
    ERRORS=$((ERRORS+1))
fi

# Check nginx
echo -n "Testing nginx... "
if command -v nginx &> /dev/null; then
    NGINX_VERSION=$(nginx -v 2>&1 | awk -F/ '{print $2}')
    echo "✅ Found ($NGINX_VERSION)"
else
    echo "❌ Not found"
    echo "   Install: brew install nginx"
    ERRORS=$((ERRORS+1))
fi

echo ""
echo "────────────────────────────────────────────────────────────────"

# Check ports
echo ""
echo "Checking if required ports are available..."

check_port() {
    PORT=$1
    NAME=$2
    echo -n "Port $PORT ($NAME)... "
    if lsof -i :$PORT > /dev/null 2>&1; then
        echo "⚠️  In use"
        lsof -i :$PORT | grep LISTEN
        return 1
    else
        echo "✅ Available"
        return 0
    fi
}

check_port 8000 "API Server" || ERRORS=$((ERRORS+1))
check_port 9002 "Extension" || ERRORS=$((ERRORS+1))
check_port 8080 "nginx" || ERRORS=$((ERRORS+1))
check_port 4040 "NGROK" || ERRORS=$((ERRORS+1))

echo ""
echo "────────────────────────────────────────────────────────────────"

# Check files
echo ""
echo "Checking required files..."

check_file() {
    FILE=$1
    NAME=$2
    echo -n "$NAME... "
    if [ -f "$FILE" ]; then
        echo "✅ Exists"
        return 0
    else
        echo "❌ Missing"
        return 1
    fi
}

check_file "nginx.conf" "nginx config"
check_file "launch_unified.sh" "Launch script"
check_file "requirements.txt" "Python requirements"
check_file "application.py" "FastAPI app"
check_file "email/block.json" "Extension config" || ERRORS=$((ERRORS+1))
check_file "email/frontend/index.js" "Extension frontend" || ERRORS=$((ERRORS+1))

echo ""
echo "────────────────────────────────────────────────────────────────"

# Check virtual environment
echo ""
echo -n "Python virtual environment... "
if [ -d ".venv" ]; then
    echo "✅ Exists"
else
    echo "⚠️  Not found (will be created on first run)"
fi

# Check Node modules
echo -n "Extension node_modules... "
if [ -d "email/node_modules" ]; then
    echo "✅ Exists"
else
    echo "⚠️  Not found (will be installed on first run)"
fi

echo ""
echo "════════════════════════════════════════════════════════════════"

if [ $ERRORS -eq 0 ]; then
    echo "✅ All tests passed! You're ready to run:"
    echo ""
    echo "   ./launch_unified.sh"
    echo ""
    exit 0
else
    echo "❌ $ERRORS error(s) found. Please fix them before running."
    echo ""
    echo "Common fixes:"
    echo "  - Install nginx: brew install nginx"
    echo "  - Install ngrok: brew install ngrok"
    echo "  - Install Airtable CLI: npm install -g @airtable/blocks-cli"
    echo "  - Free up ports: lsof -i :<port> then kill <PID>"
    echo ""
    exit 1
fi
