# Makefile for Company Research Agent + Airtable Extension
# Alternative to launch_docker.sh for those who prefer make commands

.PHONY: help start stop restart logs status backend extension ngrok build clean test

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Company Research Agent + Airtable Extension"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
	@echo ""
	@echo "Or use: ./launch_docker.sh [command]"

preflight: ## Run Python and Node preflight checks
	@echo "🔎 Running preflight checks..."
	@python3 preflight_check.py

verify: preflight ## Alias for preflight

start: ## Start all services
	@echo "🚀 Starting all services..."
	@docker compose up -d
	@echo "✅ Services started"
	@make status

stop: ## Stop all services
	@echo "🛑 Stopping all services..."
	@docker compose down
	@echo "✅ Services stopped"

restart: ## Restart all services
	@echo "🔄 Restarting services..."
	@docker compose restart
	@echo "✅ Services restarted"

logs: ## Show logs from all services
	@docker compose logs -f

status: ## Show status of all services
	@echo "📊 Service Status:"
	@docker compose ps
	@echo ""
	@echo "Getting ngrok URL..."
	@sleep 2
	@-curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1

backend: ## Show backend logs
	@docker compose logs -f backend

extension: ## Show Airtable extension logs
	@docker compose logs -f airtable-extension

ngrok: ## Show ngrok logs and URL
	@docker compose logs ngrok
	@echo ""
	@echo "Ngrok URL:"
	@curl -s http://localhost:4040/api/tunnels 2>/dev/null | grep -o '"public_url":"https://[^"]*' | grep -o 'https://[^"]*' | head -n 1

build: ## Rebuild Docker images
	@echo "🔨 Building Docker images..."
	@docker compose build --no-cache
	@echo "✅ Build complete"

clean: ## Stop and remove all containers and volumes
	@echo "🧹 Cleaning up..."
	@docker compose down -v --remove-orphans
	@echo "✅ Cleanup complete"

test: ## Test the Docker setup
	@./test_docker_setup.sh

shell-backend: ## Open shell in backend container
	@docker compose exec backend bash

shell-extension: ## Open shell in extension container
	@docker compose exec airtable-extension sh

ps: ## Show running containers
	@docker compose ps

top: ## Show resource usage
	@docker stats --no-stream

inspect: ## Show detailed service info
	@docker compose config
