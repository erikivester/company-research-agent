#!/bin/bash

# Docker Launcher for Company Research Agent + Airtable Extension
# This script manages the Docker containers for all services

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}🐳 Company Research Agent + Airtable Extension Docker Launcher${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Function to display usage
usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start     - Start all services (backend, extension, ngrok)"
    echo "  stop      - Stop all services"
    echo "  restart   - Restart all services"
    echo "  logs      - Show logs from all services"
    echo "  status    - Show status of all services"
    echo "  backend   - Show backend logs only"
    echo "  extension - Show Airtable extension logs only"
    echo "  ngrok     - Show ngrok logs and get public URL"
    echo "  build     - Rebuild all Docker images"
    echo "  verify    - Run preflight checks (no containers)"
    echo "  clean     - Stop services and remove containers/volumes"
    echo ""
    exit 1
}

# Check if .env file exists
check_env() {
    if [ ! -f ".env" ]; then
        echo -e "${YELLOW}⚠️  Warning: .env file not found${NC}"
        echo -e "${YELLOW}   Services may have missing configuration.${NC}"
        echo ""
    fi
}

# Check if ngrok token is set
check_ngrok() {
    if ! grep -q "NGROK_AUTHTOKEN" .env 2>/dev/null; then
        echo -e "${YELLOW}⚠️  Warning: NGROK_AUTHTOKEN not found in .env${NC}"
        echo -e "${YELLOW}   Get your token from: https://dashboard.ngrok.com/get-started/your-authtoken${NC}"
        echo -e "${YELLOW}   Add it to .env: NGROK_AUTHTOKEN=your_token_here${NC}"
        echo ""
    fi
}

# Get ngrok public URL
get_ngrok_url() {
    echo -e "${BLUE}🔍 Fetching ngrok URL...${NC}"
    sleep 3
    
    for i in {1..10}; do
        NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1)
        if [ ! -z "$NGROK_URL" ]; then
            echo -e "${GREEN}✅ Ngrok URL: $NGROK_URL${NC}"
            echo ""
            echo -e "${YELLOW}📋 SETUP INSTRUCTIONS:${NC}"
            echo -e "${YELLOW}   1. Open Airtable and go to your base${NC}"
            echo -e "${YELLOW}   2. Add the extension (if not already added)${NC}"
            echo -e "${YELLOW}   3. Click Settings (⚙️) in the extension${NC}"
            echo -e "${YELLOW}   4. Paste this URL in 'API Endpoint': $NGROK_URL${NC}"
            echo -e "${YELLOW}   5. Configure your table and field mappings${NC}"
            echo -e "${YELLOW}   6. Save and start generating emails!${NC}"
            echo ""
            return 0
        fi
        sleep 2
    done
    
    echo -e "${RED}❌ Could not retrieve ngrok URL${NC}"
    echo -e "${YELLOW}   Check ngrok logs: docker compose logs ngrok${NC}"
    return 1
}

# Start services
start_services() {
    check_env
    check_ngrok
    
    # Optional preflight before starting (set PRECHECK=1)
    if [ "${PRECHECK:-0}" = "1" ]; then
        echo -e "${BLUE}🔎 Running preflight checks (PRECHECK=1)...${NC}"
        if ! python3 preflight_check.py; then
            echo -e "${RED}❌ Preflight failed. Aborting start.${NC}"
            exit 1
        fi
    fi

    echo -e "${GREEN}🚀 Starting all services...${NC}"
    docker compose up -d
    
    echo ""
    echo -e "${GREEN}✅ Services started!${NC}"
    echo ""
    echo -e "${BLUE}📊 Service URLs:${NC}"
    echo -e "   🏠 Backend API:        http://localhost:8000"
    echo -e "   📈 Metrics:            http://localhost:8001"
    echo -e "   🧩 Extension Server:   http://localhost:9000"
    echo -e "   🌐 Ngrok Dashboard:    http://localhost:4040"
    echo ""
    
    get_ngrok_url
    
    echo -e "${BLUE}📝 Useful Commands:${NC}"
    echo -e "   View logs:       ./launch_docker.sh logs"
    echo -e "   Check status:    ./launch_docker.sh status"
    echo -e "   Stop services:   ./launch_docker.sh stop"
    echo ""
}

# Stop services
stop_services() {
    echo -e "${YELLOW}🛑 Stopping all services...${NC}"
    docker compose down
    echo -e "${GREEN}✅ All services stopped${NC}"
}

# Restart services
restart_services() {
    echo -e "${YELLOW}🔄 Restarting all services...${NC}"
    docker compose restart
    echo -e "${GREEN}✅ Services restarted${NC}"
    echo ""
    get_ngrok_url
}

# Show logs
show_logs() {
    docker compose logs -f "$@"
}

# Show status
show_status() {
    echo -e "${BLUE}📊 Service Status:${NC}"
    docker compose ps
    echo ""
    get_ngrok_url
}

# Build services
build_services() {
    echo -e "${BLUE}🔨 Building Docker images...${NC}"
    docker compose build --no-cache
    echo -e "${GREEN}✅ Build complete${NC}"
}

# Clean up
clean_services() {
    echo -e "${RED}🧹 Cleaning up containers and volumes...${NC}"
    docker compose down -v --remove-orphans
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

# Main command handling
case "${1:-}" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    logs)
        shift
        show_logs "$@"
        ;;
    status)
        show_status
        ;;
    backend)
        show_logs backend
        ;;
    extension)
        show_logs airtable-extension
        ;;
    ngrok)
        show_logs ngrok
        echo ""
        get_ngrok_url
        ;;
    build)
        build_services
        ;;
    verify)
        python3 preflight_check.py || exit 1
        ;;
    clean)
        clean_services
        ;;
    *)
        usage
        ;;
esac
