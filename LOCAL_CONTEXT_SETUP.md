# Local Context Setup Guide

This guide explains how to use existing files from your repository to reduce Tavily API calls by reusing research data you already have.

## Overview

The system can now read local files (JSON, Markdown, TXT) from your repository and use them as research context **instead of** or **in addition to** making Tavily API calls. This is controlled per-record via an Airtable checkbox.

## Setup in Airtable

### 1. Add a Checkbox Field

In your Airtable base (`Corporate Prospects` table):

1. Add a new field called **`Use Local Context`**
2. Field type: **Checkbox**
3. Description: "Skip Tavily API and use only existing local files for research"

### 2. Configure Your Automation

Update your Airtable automation to include the checkbox value in the webhook payload:

```javascript
// In your Airtable automation script
let payload = {
    "company": inputConfig.company,
    "airtable_record_id": inputConfig.recordId,
    "google_drive_folder_url": inputConfig.folderUrl,
    "use_local_context": inputConfig.useLocalContext  // <-- ADD THIS
};
```

## File Organization

Place your research files in these directories (relative to project root):

```
company-research-agent/
├── archive/
│   ├── reports/          # JSON research reports
│   ├── pdfs/             # PDF summaries (future)
│   └── docs/             # General documentation
├── pdfs/                 # Additional PDF files
└── ...
```

### Supported File Types

Currently supported:
- **`.json`** - Research data, company info, briefings
- **`.md`** - Markdown reports and summaries
- **`.txt`** - Plain text documents

Coming soon:
- **`.pdf`** - PDF reports (parser exists, wiring in progress)

### File Naming

Files can have any name - the system will scan all files in the configured directories. For better organization, consider naming files by company:

```
archive/reports/Archer_Daniel_Midlands_2024.json
archive/reports/ADM_sustainability.md
pdfs/ADM_briefing.txt
```

## Configuration Options

### Environment Variables

You can also control this globally via `.env`:

```bash
# Enable local file scanning (in addition to or instead of Tavily)
USE_LOCAL_FILES=false

# Skip Tavily entirely for all requests (local files only)
USE_LOCAL_ONLY=false

# Directories to scan (comma-separated, relative to project root)
LOCAL_CONTEXT_DIRS=archive/reports,archive/pdfs,pdfs,archive/docs
```

### Priority Order

1. **Airtable checkbox** (`use_local_context`) - Per-record control (highest priority)
2. **`USE_LOCAL_ONLY` env var** - Global bypass of Tavily
3. **`USE_LOCAL_FILES` env var** - Global enable of local file scanning
4. **Default** - Use Tavily API only

## Usage

### Option A: Per-Record Control (Recommended)

1. In Airtable, check the **"Use Local Context"** box for a specific company
2. Trigger your automation/webhook
3. The research will use **only local files** (no Tavily calls)

Example webhook payload:
```json
{
  "company": "Archer Daniel Midlands",
  "airtable_record_id": "rec9sdJQ5SsBJ1BAQ",
  "google_drive_folder_url": "https://drive.google.com/drive/folders/...",
  "use_local_context": true
}
```

### Option B: Global Configuration

Set in your `.env` file:

```bash
# Use local files AND Tavily (supplement web search with local data)
USE_LOCAL_FILES=true
USE_LOCAL_ONLY=false

# Use ONLY local files (no Tavily calls at all)
USE_LOCAL_FILES=true
USE_LOCAL_ONLY=true
```

Then restart your server:
```bash
uvicorn application:app --host 0.0.0.0 --port 8000 --reload
```

## Testing

### Test with Local Files Only

1. Place a test file in `archive/reports/`:
   ```json
   {
     "company": "Test Company",
     "summary": "This is local test data",
     "briefings": {
       "company_overview": "Test company is a local file test..."
     }
   }
   ```

2. Trigger via curl:
   ```bash
   curl -X POST http://localhost:8000/webhook/start-research \
     -H "Content-Type: application/json" \
     -d '{
       "company": "Test Company",
       "use_local_context": true
     }'
   ```

3. Check logs for:
   ```
   📂 Loaded X local context documents for company_brief
   🚫 Local-only mode: returning local documents without Tavily calls
   ```

## How It Works

1. **Before Tavily Search**: Each researcher node checks if `use_local_context` is enabled
2. **Scan Local Directories**: Reads `.json`, `.md`, `.txt` files from configured paths
3. **Parse Content**: Converts files to structured documents with title, content, score
4. **Skip or Supplement**: 
   - If `use_local_context=true` → Use **only** local files (no Tavily)
   - If `USE_LOCAL_FILES=true` → Use local files **and** Tavily results
5. **Pipeline Continues**: Curator, Enricher, and Briefing nodes process as normal

## Benefits

- ✅ **Save API Costs** - Reuse existing research instead of re-searching
- ✅ **Faster Execution** - No network latency for Tavily calls
- ✅ **Consistent Data** - Use your curated, validated research files
- ✅ **Per-Record Control** - Choose which companies use local vs. live data
- ✅ **Easy Migration** - Gradually move from Tavily to local files

## Logs to Watch

When local context is enabled, you'll see:

```
📂 Loaded 3 local context documents for company_brief
🚫 Local-only mode: returning local documents without Tavily calls
📦 Collecting research data for Test Company:
• Company Brief: 3 documents collected
```

When using both local + Tavily:

```
📂 Loaded 2 local context documents for company_brief
✓ Found 4 documents from web search
• Company Brief: 6 documents collected (2 local + 4 web)
```

## Troubleshooting

### No local files found
- Check directory paths in `LOCAL_CONTEXT_DIRS`
- Ensure files are `.json`, `.md`, or `.txt`
- Verify files aren't empty or corrupt

### Local files ignored
- Ensure `use_local_context=true` in webhook payload
- Or set `USE_LOCAL_FILES=true` in `.env`
- Restart server after changing `.env`

### Low-quality results
- Local files should contain relevant, well-formatted content
- Use clear section headings in markdown files
- Structure JSON with `company`, `summary`, `briefings` keys

## Next Steps

Want to enhance this feature? We can add:
- **Filename matching** - Only load files whose name contains the company name
- **PDF support** - Wire in the existing PDF parser
- **Recency filtering** - Prioritize newer files
- **Custom scoring** - Weight local files by age, size, or keywords

Let me know what you'd like next!
