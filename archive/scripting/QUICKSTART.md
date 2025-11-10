# Quick Start - Running Your Custom Extension

## You're Ready to Launch! 🚀

You've just created a full-featured Airtable custom extension. Here's how to run it:

## Step 1: Start the Extension

```bash
cd scripting
block run
```

This will:
- Build the extension
- Open it in your default browser
- Connect to your Airtable base

## Step 2: First-Time Setup

When the extension opens:

1. **Click the Settings button** (⚙️) in top-right
2. **Set API Endpoint**: 
   ```
   https://futuramic-nonglandulous-senaida.ngrok-free.dev
   ```
3. **Select Table**: Choose "Corporate Prospects" (or your table name)
4. **Map Fields**: 
   - Contact Name → "Contact Name" field
   - Company Name → "Name" field
   - Contact Title → "Contact Title" field
   - Company Summary → "Company Summary" field
   - Angle for Outreach → "Angle for Outreach" field
   - Note → "Note" field
   - Research Folder URL → "Research Drive Folder" field
   - Email Draft → "Email Draft" field
   - Template Type → "Template Type" field (if you have it)

## Step 3: Use the Extension

1. **View your records** - All records from your table appear as cards
2. **Click records** to select them (multiple selection supported)
3. **Choose a template** from the dropdown
4. **Click "Generate Email"** button
5. **Wait for completion** - Progress bar shows status
6. **Check results** - Generated emails appear in your Email Draft field

## Step 4: Sync Templates (Optional)

If you have a "Template Type" single select field:

1. **Click "Sync Field"** button
2. The field options will update with templates from Google Drive
3. Now each record can use a different template!

## Features You Can Use

### ✅ What's Working Now

- **Record selection** - Visual cards you can click
- **Template fetching** - Pulls from your Google Drive via API
- **Email generation** - Creates personalized emails with AI
- **Batch processing** - Generate multiple emails at once
- **Progress tracking** - See real-time progress
- **Template sync** - Update field options automatically
- **Settings panel** - Configure everything visually

### 🎯 Advantages Over Scripts

| Feature | Scripts | Custom Extension |
|---------|---------|------------------|
| UI | ❌ Text only | ✅ Rich visual interface |
| Record selection | ❌ Manual IDs | ✅ Click to select |
| Progress | ❌ Console logs | ✅ Progress bar |
| Templates | ❌ Type manually | ✅ Dropdown selection |
| Configuration | ❌ Edit code | ✅ Settings panel |
| Batch processing | ⚠️ Limited | ✅ Full support |

## Testing Checklist

- [ ] Extension opens in browser
- [ ] Settings panel accessible
- [ ] API endpoint configured
- [ ] Fields mapped correctly
- [ ] Templates load successfully
- [ ] Records appear as cards
- [ ] Can select multiple records
- [ ] Generate button works
- [ ] Progress bar appears
- [ ] Email draft saved to record
- [ ] Template sync works (if applicable)

## Troubleshooting

### Extension won't start
```bash
# Make sure you're in the right directory
cd /Users/erikivester/company-research-agent/scripting

# Try reinstalling dependencies
npm install

# Run again
block run
```

### "Command not found: block"
```bash
# Install Airtable Blocks CLI
npm install -g @airtable/blocks-cli

# Try again
block run
```

### Can't connect to base
- Make sure you're logged into Airtable in your browser
- The CLI will prompt you to authorize
- Follow the authorization flow

### Templates not loading
```bash
# Test API endpoint
curl -H "ngrok-skip-browser-warning: true" \
  https://futuramic-nonglandulous-senaida.ngrok-free.dev/templates

# Should return JSON with templates
```

### Changes not appearing
- The extension auto-reloads on save
- If not working, refresh the browser
- Or restart `block run`

## Development Workflow

```bash
# Terminal 1: Run API server
cd /Users/erikivester/company-research-agent
source .venv/bin/activate
uvicorn application:app --reload

# Terminal 2: Run ngrok
ngrok http 8000

# Terminal 3: Run extension
cd scripting
block run
```

## Next Steps

1. ✅ **Test with one record** - Select one, generate, verify
2. ✅ **Try batch processing** - Select 3-5 records at once
3. ✅ **Sync templates** - Update field options
4. ✅ **Customize** - Adjust field mappings for your base
5. ✅ **Deploy** - When ready, build for production

## Publishing (When Ready)

To make this available to others in your workspace:

```bash
# Build for production
block release

# Follow prompts to publish
```

## Key Files

- `frontend/index.js` - Main extension code (React)
- `frontend/style.css` - Styling
- `block.json` - Extension metadata
- `README.md` - Full documentation

## Support

If you encounter issues:
1. Check browser console for errors (F12 → Console)
2. Check API server logs
3. Verify ngrok tunnel is active
4. Review field mappings in Settings

---

**Ready to go!** Run `block run` to launch your extension! 🎉
