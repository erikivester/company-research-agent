#!/bin/bash

# Test script to verify Docker setup is working correctly

set -e

echo "🧪 Testing Docker Setup..."
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to test endpoint
test_endpoint() {
    local url=$1
    local name=$2
    local expected_code=${3:-200}
    
    echo -n "Testing $name... "
    
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    
    if [ "$http_code" == "$expected_code" ] || [ "$http_code" == "200" ]; then
        echo -e "${GREEN}✅ OK (HTTP $http_code)${NC}"
        return 0
    else
        echo -e "${RED}❌ FAILED (HTTP $http_code)${NC}"
        return 1
    fi
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running${NC}"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

echo -e "${GREEN}✅ Docker is running${NC}"
echo ""

# Check if services are running
echo "Checking Docker services..."
if ! docker compose ps | grep -q "Up"; then
    echo -e "${YELLOW}⚠️  Services are not running${NC}"
    echo "Start them with: ./launch_docker.sh start"
    exit 1
fi

echo -e "${GREEN}✅ Services are running${NC}"
echo ""

# Test backend health
echo "Testing backend services..."
test_endpoint "http://localhost:8000/health" "Backend health check"
test_endpoint "http://localhost:8000/docs" "API documentation"
test_endpoint "http://localhost:8001" "Prometheus metrics"

echo ""

# Test Airtable extension server
echo "Testing Airtable extension..."
if curl -s http://localhost:9000 > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Extension server is responding${NC}"
else
    echo -e "${YELLOW}⚠️  Extension server may not be fully started yet${NC}"
fi

echo ""

# Test ngrok
echo "Testing ngrok..."
if curl -s http://localhost:4040/api/tunnels > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Ngrok is running${NC}"
    
    NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
    
    if [ ! -z "$NGROK_URL" ]; then
        echo -e "${GREEN}✅ Ngrok URL: $NGROK_URL${NC}"
        
        # Test ngrok URL
        if curl -s -H "ngrok-skip-browser-warning: true" "$NGROK_URL/health" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ Ngrok tunnel is working${NC}"
        else
            echo -e "${YELLOW}⚠️  Ngrok tunnel may not be fully ready${NC}"
        fi
    fi
else
    echo -e "${RED}❌ Ngrok is not responding${NC}"
fi

echo ""
echo "═══════════════════════════════════════"
echo -e "${GREEN}✅ Docker setup test complete!${NC}"
echo "═══════════════════════════════════════"
echo ""
echo "Next steps:"
echo "1. Configure your Airtable extension with the ngrok URL"
echo "2. View logs: ./launch_docker.sh logs"
echo "3. Check status: ./launch_docker.sh status"
