# 🐳 Docker Quick Start Guide

## 🚀 Launch All Services

```bash
./launch_docker.sh start
```

This starts:
- ✅ Backend API (port 8000)
- ✅ Airtable Extension (port 9000)
- ✅ ngrok tunnel (port 4040)

## 📋 Common Commands

| Command | Description |
|---------|-------------|
| `./launch_docker.sh start` | Start all services |
| `./launch_docker.sh stop` | Stop all services |
| `./launch_docker.sh restart` | Restart all services |
| `./launch_docker.sh logs` | View all logs |
| `./launch_docker.sh status` | Check service status |
| `./launch_docker.sh ngrok` | Get ngrok URL |
| `./launch_docker.sh build` | Rebuild containers |
| `./launch_docker.sh clean` | Remove everything |

## 🔗 Service URLs

After starting, access:

- 🏠 **Backend API**: http://localhost:8000
- 📖 **API Docs**: http://localhost:8000/docs
- 📈 **Metrics**: http://localhost:8001
- 🧩 **Extension**: http://localhost:9000
- 🌐 **Ngrok Dashboard**: http://localhost:4040
- 🌍 **Public URL**: Check output or run `./launch_docker.sh ngrok`

## ⚙️ First Time Setup

1. **Install Docker Desktop**
   - Download from [docker.com](https://docker.com)

2. **Get ngrok Token**
   - Sign up at [ngrok.com](https://ngrok.com)
   - Get token from [dashboard](https://dashboard.ngrok.com/get-started/your-authtoken)

3. **Configure Environment**
   ```bash
   # Copy example file
   cp .env.example .env
   
   # Edit and add your credentials
   nano .env  # or use your preferred editor
   ```

4. **Start Services**
   ```bash
   ./launch_docker.sh start
   ```

5. **Configure Airtable**
   - Open your Airtable base
   - Add/open the AI Email Generator extension
   - Click Settings ⚙️
   - Paste the ngrok URL (shown in terminal)
   - Configure table mappings
   - Save!

## 🧪 Test Setup

```bash
./test_docker_setup.sh
```

## 🔍 Debugging

### View specific logs
```bash
./launch_docker.sh backend     # Backend only
./launch_docker.sh extension   # Extension only
./launch_docker.sh ngrok       # ngrok only
```

### Check container status
```bash
docker compose ps
```

### Enter a container
```bash
docker compose exec backend bash
docker compose exec airtable-extension sh
```

### View resource usage
```bash
docker stats
```

## 🔄 Development Workflow

1. **Make code changes** - Files are mounted, changes reflect immediately
2. **View logs** - `./launch_docker.sh logs`
3. **Restart if needed** - `./launch_docker.sh restart`
4. **Test** - `./test_docker_setup.sh`

## 🛠️ Troubleshooting

### Service won't start
```bash
./launch_docker.sh logs
```

### Port already in use
```bash
# Find process using port
lsof -i :8000
# Kill it or stop other services
```

### Need clean slate
```bash
./launch_docker.sh clean
./launch_docker.sh build
./launch_docker.sh start
```

## 📚 More Info

See [DOCKER_SETUP.md](DOCKER_SETUP.md) for detailed documentation.
