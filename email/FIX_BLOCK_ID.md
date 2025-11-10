# Fixing the Block ID Mismatch Error

## The Problem

You're seeing this error:
```json
{
  "error": "FORBIDDEN",
  "message": "blockId mismatch. Make sure you are entering development for the correct block."
}
```

This means the extension is trying to connect to a block ID that doesn't exist or you don't have access to in your current base.

## Solution: Create a New Custom Block

Follow these steps to create a new custom block in your Airtable base:

### Step 1: Create the Block in Airtable

1. **Go to your Airtable base** (Corporate Prospects - appBOE3i5rwM1jKLT)

2. **Click "Extensions"** in the top-right corner

3. **Click "Add an extension"**

4. **Scroll down** and click **"Build a custom extension"**

5. **Name it**: "AI Email Generator"

6. **Click "Start building"**

7. **In the editor that appears**, look for the **block identifier** at the top
   - It will look like: `appBOE3i5rwM1jKLT/blkXXXXXXXXXXXXXX`
   - Copy this full identifier

### Step 2: Initialize Your Local Extension

Back in your terminal, run:

```bash
cd /Users/erikivester/company-research-agent/scripting
block init appBOE3i5rwM1jKLT/blkXXXXXXXXXXXXXX .
```

Replace `appBOE3i5rwM1jKLT/blkXXXXXXXXXXXXXX` with the actual identifier you copied.

### Step 3: Start Development

```bash
block run
```

### Step 4: Open in Airtable

1. The terminal will show: `✅ Server listening at https://localhost:9000`
2. Go back to your Airtable base
3. Click the extension you just created
4. Click "Edit extension" in the dropdown
5. It will connect to your local development server

## Alternative: Use the Existing Block

If you already have a custom block created and want to use it:

### Step 1: Get the Block Identifier

1. Go to your Airtable base
2. Open the custom block/extension
3. Click the dropdown menu on the extension
4. Click "Edit extension"
5. Look at the URL or find the block identifier (format: `appXXX/blkYYY`)

### Step 2: Update Local Config

Create or update the file `.block/remote.json`:

```json
{
    "blockId": "blkYOUR_BLOCK_ID_HERE",
    "baseId": "appBOE3i5rwM1jKLT"
}
```

### Step 3: Start Development

```bash
cd /Users/erikivester/company-research-agent/scripting
block run
```

## Quick Setup (Recommended)

Here's the easiest way:

1. **In Airtable**:
   - Extensions → Add extension → Build custom extension
   - Name: "AI Email Generator"
   - Copy the identifier shown (e.g., `appBOE3i5rwM1jKLT/blk123456789`)

2. **In Terminal**:
   ```bash
   cd /Users/erikivester/company-research-agent/scripting
   
   # Replace with YOUR block identifier
   block init appBOE3i5rwM1jKLT/blk123456789 .
   
   # Start the server
   block run
   ```

3. **Back in Airtable**:
   - The extension will now load your local code
   - You'll see the AI Email Generator interface

## What This Does

When you run `block init` with the correct identifier, it:
- Creates `.block/remote.json` with the correct IDs
- Links your local code to the Airtable block
- Enables hot-reloading during development

## Troubleshooting

### "Block already exists"
- You already have the extension created
- Just get the identifier and run `block init <identifier> .`

### "Permission denied"
- Make sure you're logged into Airtable in your browser
- Make sure you have creator/editor permissions on the base

### Still seeing "blockId mismatch"
- Delete `.block` folder: `rm -rf .block`
- Get the block identifier from Airtable
- Run `block init <identifier> .` again

## Next Steps After Setup

Once `block run` is successful:

1. ✅ Extension loads in Airtable
2. ✅ Click Settings (⚙️) to configure
3. ✅ Map fields to your base
4. ✅ Start generating emails!

---

**Need the block identifier?** Look in Airtable:
Extensions → Your custom block → Dropdown → "Edit extension" → URL or identifier shown
