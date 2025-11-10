# 🚀 Airtable Extension Quick Start with Ngrok

This guide shows you how to launch your Airtable custom extension with ngrok in one command.

## Prerequisites

Make sure you have installed:
- ✅ Python 3.8+
- ✅ Node.js and npm
- ✅ ngrok ([download](https://ngrok.com/download))
- ✅ Airtable Blocks CLI: `npm install -g @airtable/blocks-cli`

## 🎯 One-Command Launch

Run this from the project root:

```bash
./launch_airtable_extension.sh
```

This script will:
1. ✅ Check all prerequisites
2. ✅ Set up Python virtual environment
3. ✅ Install Node dependencies
4. ✅ Start FastAPI server
5. ✅ Create ngrok tunnel
6. ✅ Test API connection
7. ✅ Launch Airtable extension in browser

## 📋 After the Extension Opens

The script will display your ngrok URL. For example:
```
📡 Ngrok URL: https://abc123.ngrok-free.app
```

### In the Airtable Extension:

1. **Click the Settings button** (⚙️) in the top-right corner

2. **Set API Endpoint** to the ngrok URL shown:
   ```
   https://abc123.ngrok-free.app
   ```

3. **Select your table** from the dropdown

4. **Map your fields**:
   - Contact Name → Your "Contact Name" field
   - Company Name → Your "Name" or "Company" field
   - Contact Title → Your "Title" field
   - Company Summary → Your "Summary" field
   - Angle for Outreach → Your "Angle" field
   - Note → Your "Note" field
   - Research Folder URL → Your Google Drive folder field
   - Email Draft → Field where emails will be saved
   - Template Type → (Optional) Single select field for templates

5. **Click outside Settings** to return to main view

6. **Enter Record IDs** in the input field (comma-separated):
   ```
   rec123abc, rec456def, rec789ghi
   ```

7. **Click "Set Record IDs"** to load those records

8. **Select a template** from the dropdown

9. **Click "Generate Email"** to create personalized emails!

## 🔧 Alternative: Manual Setup

If you prefer to start services separately:

### Terminal 1: Start API with ngrok
```bash
./start_with_ngrok.sh
```

### Terminal 2: Start extension
```bash
cd scripting
block run
```

## 🛠️ Troubleshooting

### Extension won't open
```bash
# Make sure you're logged into Airtable
# The blocks CLI will prompt you to authorize
block init  # if first time
```

### "Command not found: block"
```bash
npm install -g @airtable/blocks-cli
```

### ngrok URL not working
```bash
# Check ngrok dashboard
open http://localhost:4040

# Or check the logs
tail -f ngrok.log
```

### Templates not loading
```bash
# Test the API endpoint directly
curl -H "ngrok-skip-browser-warning: true" \
  https://YOUR-NGROK-URL/templates
```

### Record IDs not working
The extension needs record IDs to be manually entered because of Airtable's security model. To get record IDs:

1. Open your Airtable base
2. Click on a record to open the expanded view
3. The URL will contain the record ID: `https://airtable.com/app.../tbl.../recXXXXXXXXXXXXXX`
4. Copy the `recXXXXXXXXXXXXXX` part
5. Paste multiple IDs separated by commas in the extension

### API authentication errors
If your API requires authentication, you may need to modify the extension to include authentication headers. Check `application.py` for authentication requirements.

## 📁 Project Structure

```
company-research-agent/
├── launch_airtable_extension.sh  # ← One-command launcher
├── start_with_ngrok.sh           # ← API + ngrok only
├── application.py                # FastAPI server
├── scripting/                    # Airtable extension
│   ├── frontend/
│   │   └── index.js             # Extension UI
│   ├── block.json               # Extension config
│   └── package.json
├── server.log                    # API logs (created on run)
└── ngrok.log                     # Ngrok logs (created on run)
```

## 🔄 Development Workflow

The extension has **hot reload** enabled:

1. Make changes to `scripting/frontend/index.js`
2. Save the file
3. Extension automatically reloads in browser
4. No need to restart!

## 🎨 Customization

### Change API Port
```bash
PORT=9000 ./launch_airtable_extension.sh
```

### Use Different ngrok Region
Edit `start_with_ngrok.sh` and add region flag:
```bash
ngrok http $PORT --region=eu
```

### Modify Field Mappings
All field mappings are configurable through the Settings panel - no code changes needed!

## 📊 Monitoring

### View Logs
```bash
# API logs
tail -f server.log

# Ngrok logs
tail -f ngrok.log

# Or view ngrok dashboard
open http://localhost:4040
```

### Check Services
```bash
# Check if services are running
lsof -i :8000  # API server
lsof -i :4040  # ngrok
```

## 🛑 Stopping Services

Press **Ctrl+C** in the terminal where you ran the launch script.

This will automatically stop:
- Airtable extension
- FastAPI server
- ngrok tunnel

## 🎯 Common Tasks

### Refresh Templates
1. Click "Refresh" button in extension
2. Latest templates from Google Drive will load

### Sync Template Field Options
1. Create a "Template Type" Single Select field in Airtable
2. Map it in Settings
3. Click "Sync Field" button
4. Field options will update with available templates

### Batch Generate Emails
1. Enter multiple record IDs: `rec1, rec2, rec3`
2. Click "Set Record IDs"
3. Select template
4. Click "Generate Email"
5. Progress bar shows status
6. All emails generated at once!

## 📚 Additional Resources

- **Full Documentation**: `scripting/README.md`
- **Template Guide**: `TEMPLATE_SYNC_GUIDE.md`
- **API Documentation**: `docs/API.md`
- **Airtable Blocks SDK**: [docs.airtable.com](https://airtable.com/developers/blocks)

## 🆘 Need Help?

1. Check browser console (F12) for errors
2. Check `server.log` for API errors
3. Check `ngrok.log` for tunnel issues
4. Verify all field mappings in Settings
5. Test API endpoint with curl (see troubleshooting above)

## ✅ Success Checklist

- [ ] Script starts without errors
- [ ] Ngrok URL displayed
- [ ] Extension opens in browser
- [ ] Settings accessible
- [ ] API endpoint saved
- [ ] Fields mapped
- [ ] Templates load successfully
- [ ] Can enter record IDs
- [ ] Email generation works
- [ ] Emails saved to Airtable

---

**Ready to launch?** Run `./launch_airtable_extension.sh` and start generating AI-powered emails! 🚀
