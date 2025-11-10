# 🚀 Quick Reference: Unified NGROK Launch

## Single Command to Run Everything

```bash
./launch_unified.sh
```

## What You Get

✅ One NGROK URL for **both** services  
✅ AI Research Agent API  
✅ Airtable Email Extension  

## URL Usage

**Same URL for everything:**
```
https://abc123.ngrok-free.app
```

- **Extension URL in Airtable**: `https://abc123.ngrok-free.app`
- **API Endpoint in Settings**: `https://abc123.ngrok-free.app` (same!)

## Quick Checks

```bash
# View logs
tail -f api-server.log
tail -f extension-server.log
tail -f nginx.log

# Check services
lsof -i :8000  # API
lsof -i :9002  # Extension
lsof -i :8080  # nginx
lsof -i :4040  # NGROK

# Stop everything
pkill -f "uvicorn"
pkill -f "block run"
pkill nginx
pkill ngrok
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Port already in use | `lsof -i :8080` then `kill <PID>` |
| nginx won't start | `nginx -t -c $(pwd)/nginx.conf` |
| Extension not loading | Check `extension-server.log` |
| API not responding | Check `api-server.log` |

## Files

- **nginx.conf** - Routing configuration
- **launch_unified.sh** - Startup script
- **UNIFIED_NGROK_GUIDE.md** - Full documentation

---

**For details, see:** `UNIFIED_NGROK_GUIDE.md`
