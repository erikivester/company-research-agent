# 🎯 How to Run Your Airtable Extension

## Quick Start (Recommended)

### Option 1: All-in-One Launch ✨
```bash
./launch_airtable_extension.sh
```
This starts **everything** (API + ngrok + extension) in one command!

### Option 2: Separate Terminals
If you prefer more control:

**Terminal 1** - Start API with ngrok:
```bash
./start_with_ngrok.sh
```

**Terminal 2** - Start extension:
```bash
cd scripting
block run
```

## 📋 What Happens

1. **Python API server** starts on port 8000
2. **Ngrok tunnel** creates public HTTPS URL
3. **Airtable extension** opens in browser
4. You configure the extension with the ngrok URL
5. Start generating emails! 🚀

## ⚙️ Configuration in Airtable

After the extension opens:

1. Click **Settings (⚙️)** button
2. Paste ngrok URL (shown in terminal):
   ```
   https://your-unique-id.ngrok-free.dev
   ```
3. Select your **table**
4. Map your **fields**:
   - Contact Name
   - Company Name
   - Contact Title
   - Company Summary
   - Angle for Outreach
   - Note
   - Research Folder URL
   - Email Draft (where output goes)
   - Template Type (optional)

## 📝 Using the Extension

1. **Enter Record IDs** (comma-separated):
   ```
   rec123abc, rec456def, rec789ghi
   ```
   
2. **Click "Set Record IDs"** to load records

3. **Select a template** from dropdown

4. **Click "Generate Email"**

5. Emails appear in your **Email Draft** field!

## 🔍 Finding Record IDs

1. Open your Airtable base
2. Click a record to expand it
3. Look at the URL:
   ```
   https://airtable.com/appXXX/tblXXX/recABCDEF123456
                                      ^^^^^^^^^^^^^^^^
                                      This is the record ID
   ```
4. Copy the `recXXXXXXXXXXXXXX` part

## 🛠️ Troubleshooting

### "Failed to fetch templates"
- Check that API server is running
- Verify ngrok URL is correct in Settings
- Test with: `curl -H "ngrok-skip-browser-warning: true" YOUR_NGROK_URL/templates`

### "Command not found: block"
```bash
npm install -g @airtable/blocks-cli
```

### ngrok URL changes
- ngrok URLs change each time you restart (free tier)
- Just update the URL in extension Settings
- Or use a static domain (paid ngrok plan)

### Permission errors
- You need Creator or Owner role in Airtable
- Check you have edit permissions on the table

## 📊 Monitoring

- **API logs**: stdout or `server.log`
- **Ngrok dashboard**: http://localhost:4040
- **Ngrok logs**: `ngrok.log`

## 🛑 Stopping

Press **Ctrl+C** in the terminal to stop all services.

## 📚 More Help

- Full guide: `NGROK_LAUNCH_GUIDE.md`
- Extension docs: `scripting/README.md`
- API docs: `docs/API.md`

---

**Questions?** Check the detailed guides or the browser console (F12) for error messages.
