# Create Your Custom Block - Step by Step

## The Issue

The URL you have (`bipCkxWmzcoN8G3U6`) is an **installed extension/app**, not a **custom development block**. 

For development, you need a block ID starting with **`blk`** (not `bip`).

## How to Create a Development Block

### Step 1: Go to Airtable

Open your base: https://airtable.com/appBOE3i5rwM1jKLT

### Step 2: Add a Custom Block

1. Click **"Extensions"** button (top-right corner, looks like a puzzle piece)
2. In the extensions panel, click **"+ Add an extension"**
3. Scroll down to the bottom
4. Click **"Build a custom extension"**

### Step 3: Set Up the Block

1. **Name**: Enter "AI Email Generator"
2. Click **"Start building"**
3. You'll see a new panel open with a code editor

### Step 4: Get the Block Identifier

In the custom block editor:
- Look at the **top of the panel** - you should see something like:
  - "Editing: AI Email Generator"
  - Or in the URL bar, look for a parameter like `?blocks=blkXXXXXXXXXXXXX`
  
The identifier will be in the format: **`appBOE3i5rwM1jKLT/blkXXXXXXXXXXXXX`**

### Step 5: Configure Your Local Extension

Once you have the block identifier (starting with `blk`), run:

```bash
cd /Users/erikivester/company-research-agent/scripting
block add-remote appBOE3i5rwM1jKLT/blkXXXXXXXXXXXXX
block run
```

### Step 6: Development Mode

When `block run` starts successfully:
1. Go back to Airtable
2. The custom block should automatically refresh
3. You'll see your AI Email Generator interface!

## Visual Guide

```
Airtable Base
  ↓
Extensions (top-right)
  ↓
+ Add an extension
  ↓
Build a custom extension (at bottom)
  ↓
Name: "AI Email Generator"
  ↓
Start building
  ↓
Copy block ID (blkXXXXXXXXXXXXX)
  ↓
Terminal: block add-remote appBOE3i5rwM1jKLT/blkXXXXXXXXXXXXX
  ↓
Terminal: block run
  ↓
✅ Extension loads with your code!
```

## Expected Block ID Format

✅ **Correct**: `appBOE3i5rwM1jKLT/blk123abc456def` (starts with `blk`)
❌ **Wrong**: `appBOE3i5rwM1jKLT/bipCkxWmzcoN8G3U6` (starts with `bip`)

The `bip` ID is for installed marketplace extensions, not custom development blocks.

## After Setup

Once configured, you can:
- Edit `frontend/index.js` - changes auto-reload
- Configure settings in the extension
- Generate emails with your AI system

---

**Next**: Create the custom block in Airtable, then tell me the block ID that starts with `blk`!
