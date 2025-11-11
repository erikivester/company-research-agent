# Airtable Automation Setup Guide

This guide explains how to set up the email generation automation in Airtable using the provided script.

## Overview

The automation script allows you to automatically generate personalized outreach emails when certain conditions are met (e.g., button click, field update, new record creation).

## Prerequisites

1. ✅ FastAPI server running (localhost:8000)
2. ✅ Ngrok tunnel active (or update the URL in the script)
3. ✅ Google Drive credentials configured
4. ✅ Airtable base with required fields

## Setup Steps

### Step 1: Create a New Automation

1. Go to your Airtable base
2. Click on "Automations" in the top menu
3. Click "Create automation"
4. Give it a descriptive name (e.g., "Generate Outreach Email")

### Step 2: Configure the Trigger

Choose one of these trigger options:

#### Option A: Button Field Trigger (Recommended)
1. Select trigger: "When button clicked"
2. Choose your table: "Corporate Prospects"
3. Select the button field (create one if needed named "Generate Email")

#### Option B: Conditional Trigger
1. Select trigger: "When record matches conditions"
2. Choose your table: "Corporate Prospects"
3. Set conditions (e.g., "Status = Ready for Outreach")

#### Option C: Manual Trigger
1. Select trigger: "When record enters view"
2. Choose your table and a specific view

### Step 3: Add Script Action

1. Click "+ Add action"
2. Choose "Run a script"
3. Copy the contents of `airtable_automation_script.js` into the code editor

### Step 4: Configure Input Variables

In the script action configuration, map these input variables:

#### Required Variables:

| Variable Name | Type | Value/Mapping |
|--------------|------|---------------|
| `recordId` | String | From trigger: `Record ID` |
| `templateType` | String | From trigger: Field value OR fixed value like `"CGF_METHANE_OPEN_CALL_CORPORATE_OUTREACH"` |
| `contactName` | String | From trigger: Field value (e.g., `Contact Name`) |

#### Optional Variables:

| Variable Name | Type | Value/Mapping |
|--------------|------|---------------|
| `apiEndpoint` | String | Fixed value: Your ngrok URL (if different from default) |

**Example Input Variable Configuration:**

```
recordId: {Record ID from trigger}
templateType: "CGF_METHANE_OPEN_CALL_CORPORATE_OUTREACH"
contactName: {Contact Name field from record}
```

### Step 5: Test the Automation

1. Click "Test" in the automation editor
2. Select a test record
3. Check the run logs for any errors
4. Verify the email draft was written to the record

### Step 6: Turn On the Automation

1. Review the settings
2. Click "Turn on automation"
3. The automation will now run automatically based on your trigger

## Field Mapping

Ensure your Airtable base has these fields (adjust names in script if different):

### Required Fields:
- **Name** (Single line text): Company name
- **Contact Name** (Single line text): Contact person's name
- **Email Draft** (Long text): Where generated email will be saved

### Recommended Fields:
- **Contact Title** (Single line text): Contact's job title
- **Company Summary** (Long text): Brief company overview
- **Angle for Outreach** (Long text): Strategic notes for personalization
- **Note** (Long text): Additional context
- **Research Drive Folder** (URL or linked record): Google Drive folder with research

### Optional Fields:
- **Generate Email** (Button): Trigger button for manual generation
- **Status** (Single select): Track outreach status

## Customization Options

### 1. Change API Endpoint

If not using ngrok or the URL changes, update this line in the script:

```javascript
const API_BASE_URL = "https://your-new-url.com";
```

Or pass it as an input variable:

```javascript
apiEndpoint: "https://your-server-url.com"
```

### 2. Use Dynamic Template Selection

Instead of hardcoding the template, you can:

1. Add a "Template Type" single-select field to your base with options like:
   - CGF_METHANE_OPEN_CALL_CORPORATE_OUTREACH
   - STANDARD_INTRO
   
2. Map the `templateType` input variable to this field:
   ```
   templateType: {Template Type field from record}
   ```

### 3. Add Field Name Mappings

If your field names differ, update these lines in the script:

```javascript
const companyName = record.getCellValue("Name") || "";
const contactTitle = record.getCellValue("Contact Title") || "";
// etc...
```

### 4. Add Conditional Logic

You can add conditions before generating the email:

```javascript
// Check if email already exists
const existingDraft = record.getCellValue("Email Draft");
if (existingDraft && existingDraft.length > 100) {
    throw new Error("Email draft already exists. Clear it first to regenerate.");
}

// Validate required fields
if (!companyName) {
    throw new Error("Company name is required");
}
```

## Troubleshooting

### Error: "Missing required input: recordId"
- Make sure you mapped the `recordId` variable in the script action
- Use `{Record ID}` from the trigger

### Error: "Record not found"
- Check that the table name matches: `base.getTable("Corporate Prospects")`
- Verify the record ID is being passed correctly

### Error: "API request failed"
- Check that your ngrok tunnel is running
- Verify the API_BASE_URL is correct
- Test the API endpoint directly with curl

### Error: "API returned error 422"
- Check that all required fields have values
- Verify the template type exists
- Check the API logs for details

### No Email Generated
- Check the automation run logs for console output
- Verify the "Email Draft" field name matches
- Check that the API is returning data

## Advanced: Multi-Step Automation

You can create a more complex workflow:

1. **Trigger**: When button clicked
2. **Action 1**: Update status field to "Generating..."
3. **Action 2**: Run script to generate email
4. **Conditional**: If script succeeds
   - **Action 3a**: Update status to "Ready to Send"
   - **Action 3b**: Send notification
5. **Conditional**: If script fails
   - **Action 4a**: Update status to "Generation Failed"
   - **Action 4b**: Send error notification

## Best Practices

1. **Test thoroughly**: Always test with sample records first
2. **Monitor runs**: Check automation run history regularly
3. **Handle errors**: The script includes error handling, but review logs
4. **Rate limiting**: Be mindful of API rate limits (60 requests/hour by default)
5. **Backup data**: The script overwrites the Email Draft field
6. **Use views**: Create filtered views for records ready for automation

## Differences from Scripting Extension

| Feature | Scripting Extension | Automation |
|---------|-------------------|------------|
| UI interaction | ✅ Yes (buttons, markdown) | ❌ No |
| User input | ✅ Interactive prompts | ✅ Pre-configured variables |
| Execution | 🔵 Manual (user clicks run) | 🟢 Automatic (trigger-based) |
| Record selection | 🔵 User selects | 🟢 From trigger |
| Output display | 🔵 Shown in UI | 🟢 Logs only |
| Use case | Interactive data exploration | Automated workflows |

## Example Workflows

### Workflow 1: Bulk Generation
1. Create a view "Ready for Email"
2. Trigger: When record enters view
3. Script generates email automatically
4. Status updates to "Email Generated"

### Workflow 2: Manual Review
1. Button field "Generate Draft"
2. Trigger: When button clicked
3. Script generates email
4. User reviews before sending

### Workflow 3: New Lead Processing
1. Trigger: When record created
2. Wait 5 minutes (for research upload)
3. Run script to generate email
4. Notify team member

## Support

If you encounter issues:
1. Check automation run logs
2. Review server logs: `tail -f logs/app.log`
3. Test the API endpoint with curl
4. Verify field names match your base

## Resources

- Airtable Automations Documentation: https://support.airtable.com/docs/getting-started-with-airtable-automations
- Scripting API Reference: https://airtable.com/developers/scripting/api
