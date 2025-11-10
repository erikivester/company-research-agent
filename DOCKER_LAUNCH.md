# 🐳 Docker Launch Instructions

## Quick Start (3 Commands)

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env and add your NGROK_AUTHTOKEN and API keys

# 2. Start services
./launch_docker.sh start

# 3. Use the ngrok URL shown in Airtable extension settings
```

## What Gets Started

When you run `./launch_docker.sh start`, these services start:

1. **Backend API** (FastAPI)
   - Main research agent API
   - Email generation endpoints
   - Template management
   - Port 8000

2. **Airtable Extension** (Node.js)
   - Block development server
   - React-based UI
   - Port 9000

3. **ngrok** (Tunnel)
   - Public HTTPS URL
   - Required for Airtable HTTPS
   - Dashboard on port 4040

## Prerequisites

- ✅ Docker Desktop installed and running
- ✅ ngrok account (free tier works)
- ✅ `.env` file configured with credentials

## All Available Commands

### Using the Shell Script
```bash
./launch_docker.sh start      # Start all services
./launch_docker.sh stop       # Stop all services
./launch_docker.sh restart    # Restart services
./launch_docker.sh logs       # View all logs
./launch_docker.sh status     # Check status
./launch_docker.sh backend    # Backend logs only
./launch_docker.sh extension  # Extension logs only
./launch_docker.sh ngrok      # Get ngrok URL
./launch_docker.sh build      # Rebuild images
./launch_docker.sh clean      # Remove everything
```

### Using Make (Alternative)
```bash
make start       # Start all services
make stop        # Stop all services
make restart     # Restart services
make logs        # View all logs
make status      # Check status
make test        # Test the setup
make help        # Show all commands
```

### Using Docker Compose Directly
```bash
docker compose up -d              # Start in background
docker compose down               # Stop services
docker compose logs -f            # Follow logs
docker compose ps                 # List services
docker compose restart backend    # Restart specific service
```

## Configuration Steps

### 1. Get ngrok Token
1. Go to https://ngrok.com
2. Sign up for free account
3. Get your auth token from https://dashboard.ngrok.com/get-started/your-authtoken
4. Add to `.env`: `NGROK_AUTHTOKEN=your_token_here`

### 2. Configure .env File
```bash
# Copy example
cp .env.example .env

# Edit with your values
nano .env  # or code .env
```

Required variables:
- `NGROK_AUTHTOKEN` - Your ngrok token
- `OPENAI_API_KEY` - OpenAI API key
- `TAVILY_API_KEY` - Tavily API key
- `ANTHROPIC_API_KEY` - Anthropic API key (optional)
- `MONGO_URI` - MongoDB connection string
- `JWT_SECRET_KEY` - Secret for JWT tokens
- `API_KEY` - API key for authentication
- `AIRTABLE_API_KEY` - Airtable API key
- `AIRTABLE_BASE_ID` - Your Airtable base ID

### 3. Start Services
```bash
./launch_docker.sh start
```

Wait for:
- ✅ Backend health check passes
- ✅ Extension server starts
- ✅ ngrok tunnel establishes
- 📋 Copy the ngrok URL shown

### 4. Configure Airtable Extension
1. Open your Airtable base
2. Add or open "AI Email Generator" extension
3. Click Settings (⚙️ gear icon)
4. Paste the ngrok URL in "API Endpoint"
5. Select your table
6. Map fields (Company, Context, etc.)
7. Save settings
8. Start generating emails!

## Testing the Setup

Run the automated test:
```bash
./test_docker_setup.sh
```

This checks:
- ✅ Docker is running
- ✅ Services are up
- ✅ Backend health check
- ✅ API endpoints responding
- ✅ Extension server running
- ✅ ngrok tunnel working
- ✅ Public URL accessible

## Accessing Services

Once running, access:

| Service | URL | Description |
|---------|-----|-------------|
| Backend API | http://localhost:8000 | Main API |
| API Docs | http://localhost:8000/docs | Swagger UI |
| Metrics | http://localhost:8001 | Prometheus metrics |
| Extension | http://localhost:9000 | Dev server |
| ngrok Dashboard | http://localhost:4040 | Tunnel info |
| Public API | https://xxxx.ngrok.io | External access |

## Troubleshooting

### Services won't start
```bash
# Check logs
./launch_docker.sh logs

# Check Docker
docker ps
docker compose ps
```

### Port already in use
```bash
# Find what's using port 8000
lsof -i :8000

# Kill the process or stop the service
kill -9 <PID>
```

### Can't get ngrok URL
```bash
# Check ngrok logs
./launch_docker.sh ngrok

# Verify token in .env
grep NGROK_AUTHTOKEN .env

# Check ngrok dashboard
open http://localhost:4040
```

### Backend health check fails
```bash
# Check backend logs
./launch_docker.sh backend

# Test directly
curl http://localhost:8000/health
```

### Extension not loading
```bash
# Check extension logs
./launch_docker.sh extension

# Check if server is running
curl http://localhost:9000
```

### Need to rebuild
```bash
# Clean rebuild
./launch_docker.sh clean
./launch_docker.sh build
./launch_docker.sh start
```

## Development Workflow

1. **Make code changes**
   - Backend: Edit files in `./backend/` or `application.py`
   - Extension: Edit files in `./scripting/`
   - Changes reflect immediately (hot reload)

2. **View logs**
   ```bash
   ./launch_docker.sh logs
   ```

3. **Test changes**
   ```bash
   ./test_docker_setup.sh
   ```

4. **Restart if needed**
   ```bash
   ./launch_docker.sh restart
   ```

## Stopping Services

```bash
# Graceful stop
./launch_docker.sh stop

# Or with make
make stop

# Or with docker compose
docker compose down
```

## Complete Cleanup

```bash
# Remove containers, volumes, and networks
./launch_docker.sh clean

# Or
make clean
```

## Monitoring

### View logs
```bash
./launch_docker.sh logs           # All services
./launch_docker.sh backend        # Backend only
./launch_docker.sh extension      # Extension only
```

### Check resource usage
```bash
docker stats
```

### Health checks
```bash
curl http://localhost:8000/health      # Backend
curl http://localhost:9000             # Extension
curl http://localhost:4040/api/tunnels # ngrok
```

## Documentation

- **DOCKER_QUICKSTART.md** - Quick reference guide
- **DOCKER_SETUP.md** - Comprehensive documentation
- **DOCKER_IMPLEMENTATION.md** - Architecture and implementation details

## Production Notes

⚠️ This setup is for **development only**. For production:
- Replace ngrok with proper reverse proxy
- Use managed certificates (Let's Encrypt)
- Implement secrets management
- Set resource limits
- Enable proper monitoring
- Use production database
- See DOCKER_SETUP.md for details

## Support

Having issues?
1. Run `./test_docker_setup.sh` to diagnose
2. Check `./launch_docker.sh logs` for errors
3. Review DOCKER_SETUP.md for detailed troubleshooting
4. Verify all environment variables in `.env`

## Summary

```bash
# Complete workflow
cp .env.example .env              # 1. Configure
./launch_docker.sh start          # 2. Start
./test_docker_setup.sh            # 3. Test
# Use ngrok URL in Airtable       # 4. Configure
./launch_docker.sh logs           # 5. Monitor
./launch_docker.sh stop           # 6. Stop when done
```

That's it! 🎉
