# Docker Setup for Company Research Agent + Airtable Extension

This Docker setup allows you to run the entire stack including:
- **Backend API** (FastAPI server on port 8000)
- **Airtable Extension** (Development server on port 9000)
- **ngrok** (Public HTTPS tunnel for external access)

## Prerequisites

1. **Docker** and **Docker Compose** installed
   - [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)

2. **ngrok Account** (for external access)
   - Sign up at [ngrok.com](https://ngrok.com/)
   - Get your auth token from [dashboard](https://dashboard.ngrok.com/get-started/your-authtoken)

3. **Environment Variables**
   - Copy `.env.example` to `.env` (if available) or create `.env`
   - Add your ngrok token: `NGROK_AUTHTOKEN=your_token_here`
   - Add other required API keys and credentials

## Quick Start

### 1. First Time Setup

```bash
# Make the launch script executable (already done)
chmod +x launch_docker.sh

# Add your ngrok token to .env
echo "NGROK_AUTHTOKEN=your_token_here" >> .env

# Build and start all services
./launch_docker.sh start
```

### 2. Get the Public URL

After starting, the script will display:
- Backend API URL (http://localhost:8000)
- Ngrok public URL (https://xxxx.ngrok.io)
- Ngrok dashboard (http://localhost:4040)

### 3. Configure Airtable Extension

1. Open your Airtable base
2. Add or open the AI Email Generator extension
3. Click Settings (⚙️)
4. Paste the ngrok URL in "API Endpoint"
5. Configure table and field mappings
6. Save and start using!

## Available Commands

```bash
# Start all services
./launch_docker.sh start

# Stop all services
./launch_docker.sh stop

# Restart services
./launch_docker.sh restart

# View all logs (live)
./launch_docker.sh logs

# View specific service logs
./launch_docker.sh backend
./launch_docker.sh extension
./launch_docker.sh ngrok

# Check service status
./launch_docker.sh status

# Rebuild Docker images
./launch_docker.sh build

# Clean up everything (removes containers and volumes)
./launch_docker.sh clean
```

## Architecture

### Services

1. **backend** - FastAPI application
   - Port 8000: Main API
   - Port 8001: Prometheus metrics
   - Health check: `http://localhost:8000/health`

2. **airtable-extension** - Airtable block development server
   - Port 9000: Extension dev server
   - Proxied through backend at `/block/*`

3. **ngrok** - Public HTTPS tunnel
   - Port 4040: Web dashboard
   - Creates secure tunnel to backend:8000

### Network

All services communicate via the `app-network` Docker network.

### Volumes

- `./backend` - Backend code (live reload)
- `./application.py` - Main application file (live reload)
- `./scripting` - Extension code (live reload)
- `./pdfs` - Generated PDF reports
- `./reports` - Generated reports
- `./gdrive_credentials.json` - Google Drive credentials
- `./client_secret.json` - OAuth client secrets

## Troubleshooting

### Container won't start

```bash
# Check logs
./launch_docker.sh logs

# Check specific service
./launch_docker.sh backend
```

### Can't get ngrok URL

```bash
# Check ngrok logs
./launch_docker.sh ngrok

# Verify your token in .env
grep NGROK_AUTHTOKEN .env
```

### Backend health check failing

```bash
# Check if backend is running
curl http://localhost:8000/health

# View backend logs
./launch_docker.sh backend
```

### Extension not loading

```bash
# Check extension server logs
./launch_docker.sh extension

# Verify it's running
curl http://localhost:9000
```

### Need to rebuild

```bash
# Rebuild all images
./launch_docker.sh build

# Then restart
./launch_docker.sh start
```

## Development Workflow

### Making Code Changes

The volumes are mounted for live reload:

1. **Backend changes**: Edit files in `./backend/` or `application.py`
   - Changes reflected immediately (FastAPI auto-reload)

2. **Extension changes**: Edit files in `./scripting/`
   - May need to refresh Airtable extension

### Viewing Logs

```bash
# All services (follow mode)
./launch_docker.sh logs

# Just backend
./launch_docker.sh backend

# Just extension
./launch_docker.sh extension
```

### Monitoring

- **Application Metrics**: http://localhost:8001
- **Ngrok Dashboard**: http://localhost:4040
- **API Docs**: http://localhost:8000/docs

## Environment Variables

Required variables in `.env`:

```bash
# ngrok
NGROK_AUTHTOKEN=your_ngrok_token

# API Keys
OPENAI_API_KEY=your_openai_key
ANTHROPIC_API_KEY=your_anthropic_key
TAVILY_API_KEY=your_tavily_key

# MongoDB
MONGO_URI=your_mongodb_uri

# JWT
JWT_SECRET_KEY=your_jwt_secret

# Airtable
AIRTABLE_API_KEY=your_airtable_key
AIRTABLE_BASE_ID=your_base_id

# Google Drive (optional)
EMAIL_TEMPLATES_FOLDER_ID=your_folder_id
```

## Production Considerations

This setup is for **development** purposes. For production:

1. Replace ngrok with a proper reverse proxy (nginx, Traefik)
2. Use managed certificates (Let's Encrypt)
3. Set up proper secrets management
4. Use production-grade databases
5. Configure proper logging and monitoring
6. Set resource limits in docker-compose.yml
7. Use Docker secrets instead of .env files
8. Enable HTTPS only
9. Implement rate limiting
10. Set up proper backup strategies

## Maintenance

### Update Dependencies

```bash
# Update Python packages
pip install -r requirements.txt --upgrade
pip freeze > requirements.txt

# Rebuild containers
./launch_docker.sh build
./launch_docker.sh start
```

### Clean Up

```bash
# Remove containers and volumes
./launch_docker.sh clean

# Remove unused Docker resources
docker system prune -a
```

## Support

For issues:
1. Check logs: `./launch_docker.sh logs`
2. Check status: `./launch_docker.sh status`
3. Review the troubleshooting section above
4. Check Docker logs: `docker compose logs`

## License

[Your License Here]
