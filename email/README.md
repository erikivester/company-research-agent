# AI Email Generator - Airtable Custom Extension

A powerful Airtable custom extension that generates personalized outreach emails using AI, combining templates from Google Drive with research context and Airtable data.

## Features

✨ **Visual Interface** - Easy-to-use UI with record selection and template management
🎯 **Dynamic Templates** - Fetch and sync templates directly from Google Drive
🤖 **AI-Powered** - Generate personalized emails using OpenAI GPT-4
📊 **Batch Processing** - Generate emails for multiple records at once
🔄 **Template Sync** - Keep Airtable field options synchronized with available templates
⚙️ **Configurable** - Flexible field mapping for any base structure
📈 **Progress Tracking** - Real-time progress indicators for batch operations

## Screenshots

### Main Interface
- Record selection with visual cards
- Template dropdown or field-based selection
- Batch email generation with progress tracking

### Settings Panel
- API endpoint configuration
- Table and field mapping
- Template field setup

## Installation

### Prerequisites

1. **Airtable Blocks CLI** installed:
   ```bash
   npm install -g @airtable/blocks-cli
   ```

2. **FastAPI Server** running with endpoints:
   - `GET /templates` - List available templates
   - `POST /generate-outreach` - Generate emails

3. **Ngrok** tunnel (for development) or production URL

### Setup Steps

1. **Navigate to the extension directory**:
   ```bash
   cd scripting
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Initialize the extension** (first time only):
   ```bash
   block run
   ```

4. **Open in Airtable**:
   - The extension will open in your browser
   - Click "Add extension to base" if prompted

5. **Configure settings**:
   - Click the settings (⚙️) button
   - Set your API endpoint
   - Map fields to your base structure

## Configuration

### Required Fields

Your Airtable base should have these fields (names can be customized):

| Field | Type | Purpose |
|-------|------|---------|
| Contact Name | Single line text | Name of the person to email |
| Company Name | Single line text | Company name |
| Contact Title | Single line text | Contact's job title |
| Company Summary | Long text | Brief company overview |
| Angle for Outreach | Long text | Strategic notes |
| Note | Long text | Additional context |
| Research Folder URL | URL | Google Drive folder with research |
| Email Draft | Long text | Where generated email is saved |
| Template Type | Single select | (Optional) Dynamic template selection |

### API Endpoint

Default: `https://futuramic-nonglandulous-senaida.ngrok-free.dev`

Update this in Settings to match your:
- Ngrok tunnel URL (development)
- Production server URL
- Localhost (if running in same network)

## Usage

### Basic Workflow

1. **Open the extension** in your Airtable base

2. **Select records** - Click on record cards to select multiple records for batch processing

3. **Choose template**:
   - **Manual**: Select from dropdown (if no Template Type field configured)
   - **Dynamic**: Each record uses its own Template Type field value

4. **Click "Generate Email"** - The extension will:
   - Fetch templates from API
   - Pull research context from Google Drive
   - Generate personalized emails with AI
   - Save drafts to the Email Draft field

5. **Review drafts** - Check generated emails in your records

### Template Management

#### Refresh Templates
- Click "Refresh" to fetch latest templates from Google Drive
- Updates the dropdown with current options

#### Sync Template Field
- Click "Sync Field" to update your Template Type single select options
- Adds new templates from Drive
- Removes templates no longer available
- Preserves colors for existing options

### Batch Processing

Generate emails for multiple records:
1. Select multiple records (click each card)
2. All selected records will be processed
3. Progress bar shows real-time status
4. Success/error summary displayed when complete

## Advanced Features

### Dynamic Template Selection

Enable per-record template selection:

1. **Create field**: Add "Template Type" single select field
2. **Configure**: Map it in Settings → Template Type Field
3. **Sync**: Click "Sync Field" to populate options from API
4. **Use**: Each record can now use a different template

### Field Mapping Flexibility

The extension works with any base structure:
- All field mappings are configurable
- Use your existing field names
- Map only the fields you need

### Error Handling

The extension handles common errors gracefully:
- ❌ API connection failures
- ❌ Missing required fields
- ❌ Rate limiting
- ❌ Invalid templates

Error messages provide clear guidance for resolution.

## Development

### Local Development

1. **Start the extension**:
   ```bash
   cd scripting
   block run
   ```

2. **Make changes** to `frontend/index.js`

3. **Hot reload** - Changes appear automatically in Airtable

### File Structure

```
scripting/
├── frontend/
│   ├── index.js          # Main React component
│   └── style.css         # Styling
├── block.json            # Extension metadata
├── package.json          # Dependencies
└── .eslintrc.js         # Linting rules
```

### Key Components

**EmailGeneratorApp** - Main app with settings toggle
**SettingsPanel** - Configuration interface
**MainPanel** - Email generation interface

### API Integration

The extension makes two main API calls:

```javascript
// Fetch available templates
GET /templates
Response: { "TEMPLATE_NAME": "Description..." }

// Generate email
POST /generate-outreach
Body: {
  template_type: string,
  contact_name: string,
  airtable_context: { ... },
  google_drive_folder_url: string
}
Response: {
  email_text: string,
  template_used: string,
  context_used: { ... }
}
```

## Troubleshooting

### "Failed to fetch templates"

**Causes**:
- API server not running
- Ngrok tunnel down
- Wrong API endpoint in settings

**Fix**:
```bash
# Check server
curl -H "ngrok-skip-browser-warning: true" \
  YOUR_URL/templates

# Should return JSON with templates
```

### "API error: 422"

**Causes**:
- Missing required fields
- Invalid data format
- Template not found

**Fix**:
- Check all required fields have values
- Verify template name matches exactly
- Check API logs for details

### Template sync fails

**Causes**:
- Template field is not Single Select type
- Field locked or in use by views
- Permission issues

**Fix**:
- Ensure field type is "Single Select"
- Temporarily remove from filtered views
- Check you have edit permissions

### Records not showing

**Causes**:
- Table not selected in settings
- Field mappings incomplete

**Fix**:
- Go to Settings (⚙️)
- Select table
- Map all required fields

## Best Practices

### 1. Configure Before Use
- Set up all field mappings in Settings first
- Test with one record before batch processing
- Verify API endpoint is correct

### 2. Template Management
- Sync template field regularly (weekly or after adding templates)
- Use clear, descriptive template names
- Keep templates organized in Google Drive

### 3. Batch Processing
- Start with small batches (5-10 records)
- Monitor progress for errors
- Review generated emails before sending

### 4. Field Validation
- Ensure required fields have values
- Use views to filter ready-for-email records
- Validate research folder URLs are correct

## API Requirements

Your FastAPI server must provide:

### GET /templates
Returns available email templates from Google Drive.

**Headers**: 
- `ngrok-skip-browser-warning: true` (for ngrok)

**Response**:
```json
{
  "TEMPLATE_NAME_1": "Template description...",
  "TEMPLATE_NAME_2": "Another template..."
}
```

### POST /generate-outreach
Generates personalized email using AI.

**Headers**:
- `Content-Type: application/json`
- `ngrok-skip-browser-warning: true` (for ngrok)

**Body**:
```json
{
  "template_type": "TEMPLATE_NAME",
  "contact_name": "John Doe",
  "airtable_context": {
    "name": "Company Name",
    "title": "Job Title",
    "summary": "Company summary",
    "angle_for_outreach": "Strategic angle",
    "note": "Additional notes"
  },
  "google_drive_folder_url": "https://drive.google.com/..."
}
```

**Response**:
```json
{
  "email_text": "Generated email content...",
  "template_used": "TEMPLATE_NAME",
  "context_used": {
    "template": true,
    "research": true,
    "airtable": true
  }
}
```

## Comparison: Extension vs Automations

| Feature | Custom Extension | Automations |
|---------|-----------------|-------------|
| **UI** | ✅ Rich visual interface | ❌ No UI |
| **Batch processing** | ✅ Select multiple records | ❌ One at a time |
| **Real-time feedback** | ✅ Progress bars, status | ⚠️ Logs only |
| **Template preview** | ✅ See all templates | ❌ Text input only |
| **Field sync** | ✅ One-click sync | ⚠️ Separate script |
| **User interaction** | ✅ Interactive | ❌ Automatic |
| **Execution** | 🔵 Manual trigger | 🟢 Automatic |

**Use Extension when**: You want interactive control and visual feedback
**Use Automation when**: You want scheduled/automatic execution

## Updates & Maintenance

### Updating the Extension

```bash
# Pull latest changes
git pull origin main

# Install dependencies
cd scripting
npm install

# Deploy
block run
```

### Monitoring

Check for:
- API endpoint changes (update in Settings)
- New template additions (sync field)
- Base structure changes (remap fields)
- Error patterns in Airtable logs

## Support

### Documentation
- **Setup Guide**: `/AIRTABLE_AUTOMATION_SETUP.md`
- **Template Sync**: `/TEMPLATE_SYNC_GUIDE.md`
- **API Docs**: `/docs/API.md`

### Common Issues
1. API connection → Check server and ngrok
2. Field errors → Verify field mappings
3. Template issues → Refresh or sync templates

## License

MIT License - See LICENSE.md

## Credits

Built with:
- Airtable Blocks SDK
- React
- FastAPI
- OpenAI GPT-4

---

**Version**: 1.0.0
**Last Updated**: November 10, 2025
