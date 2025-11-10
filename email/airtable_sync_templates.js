/**
 * Airtable Template Sync Script
 * 
 * This script fetches available email templates from your API and updates
 * the "Template Type" single select field options in Airtable.
 * 
 * USE CASES:
 * 1. Run manually when you add new templates to Google Drive
 * 2. Schedule as a daily automation to keep options in sync
 * 3. Run before generating emails to ensure options are current
 * 
 * SETUP:
 * - Can run as a standalone script in Airtable Scripting Extension
 * - Or as the first step in an automation (before email generation)
 * 
 * CONFIGURATION:
 * - Update API_BASE_URL to your server endpoint
 * - Update TABLE_NAME and FIELD_NAME if different
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

const API_BASE_URL = "https://futuramic-nonglandulous-senaida.ngrok-free.dev";
const TABLE_NAME = "Corporate Prospects";
const FIELD_NAME = "Template Type";

// Color palette for options (Airtable single select colors)
const COLORS = [
    "blueLight2",
    "cyanLight2", 
    "tealLight2",
    "greenLight2",
    "yellowLight2",
    "orangeLight2",
    "redLight2",
    "pinkLight2",
    "purpleLight2",
    "grayLight2"
];

// ============================================================================
// FETCH AVAILABLE TEMPLATES FROM API
// ============================================================================

console.log("🔍 Fetching available templates from API...");
console.log(`API Endpoint: ${API_BASE_URL}/templates`);

let templatesResponse;
try {
    templatesResponse = await fetch(`${API_BASE_URL}/templates`, {
        method: "GET",
        headers: {
            "ngrok-skip-browser-warning": "true"
        }
    });
} catch (error) {
    throw new Error(`Failed to connect to API: ${error.message}\n\nMake sure:\n1. Server is running\n2. Ngrok tunnel is active\n3. API_BASE_URL is correct`);
}

if (!templatesResponse.ok) {
    const errorText = await templatesResponse.text();
    throw new Error(`API returned error ${templatesResponse.status}: ${errorText}`);
}

const templates = await templatesResponse.json();
const templateNames = Object.keys(templates);

console.log(`✓ Found ${templateNames.length} templates:`);
templateNames.forEach((name, index) => {
    const description = templates[name].substring(0, 60);
    console.log(`  ${index + 1}. ${name}`);
    console.log(`     └─ ${description}${templates[name].length > 60 ? '...' : ''}`);
});

if (templateNames.length === 0) {
    throw new Error("No templates found in Google Drive. Please add templates first.");
}

// ============================================================================
// GET CURRENT FIELD CONFIGURATION
// ============================================================================

const table = base.getTable(TABLE_NAME);
const field = table.getField(FIELD_NAME);

console.log("\n📋 Current field configuration:");
console.log(`   Table: ${TABLE_NAME}`);
console.log(`   Field: ${FIELD_NAME}`);
console.log(`   Field Type: ${field.type}`);

// Verify it's a single select field
if (field.type !== "singleSelect") {
    throw new Error(`Field "${FIELD_NAME}" must be a Single Select field. Current type: ${field.type}`);
}

// Get current options
const currentOptions = field.options.choices;
const currentNames = currentOptions.map(opt => opt.name);

console.log(`   Current options: ${currentOptions.length}`);
currentNames.forEach((name, index) => {
    console.log(`     ${index + 1}. ${name}`);
});

// ============================================================================
// DETERMINE WHAT NEEDS TO BE UPDATED
// ============================================================================

console.log("\n🔄 Analyzing changes needed...");

// Find new templates (in API but not in Airtable)
const newTemplates = templateNames.filter(name => !currentNames.includes(name));

// Find obsolete templates (in Airtable but not in API)
const obsoleteTemplates = currentNames.filter(name => !templateNames.includes(name));

// Find unchanged templates
const unchangedTemplates = templateNames.filter(name => currentNames.includes(name));

console.log(`   ✅ Unchanged: ${unchangedTemplates.length}`);
if (unchangedTemplates.length > 0) {
    unchangedTemplates.forEach(name => console.log(`      - ${name}`));
}

console.log(`   ➕ To add: ${newTemplates.length}`);
if (newTemplates.length > 0) {
    newTemplates.forEach(name => console.log(`      + ${name}`));
}

console.log(`   ➖ To remove: ${obsoleteTemplates.length}`);
if (obsoleteTemplates.length > 0) {
    obsoleteTemplates.forEach(name => console.log(`      - ${name}`));
}

// ============================================================================
// CHECK IF UPDATE IS NEEDED
// ============================================================================

if (newTemplates.length === 0 && obsoleteTemplates.length === 0) {
    console.log("\n✓ Field options are already in sync! No update needed.");
    output.markdown("## ✅ Templates Already Synced\n\nThe field options match the available templates.");
} else {
    // ============================================================================
    // UPDATE FIELD OPTIONS
    // ============================================================================
    
    console.log("\n🔧 Updating field options...");
    
    // Build new choices array
    // Keep existing options that are still valid (preserves colors and order)
    const newChoices = [];
    
    // First, add all existing valid options (preserves their colors)
    currentOptions.forEach(option => {
        if (templateNames.includes(option.name)) {
            newChoices.push({
                name: option.name,
                color: option.color
            });
        }
    });
    
    // Then add new templates with assigned colors
    newTemplates.forEach((name, index) => {
        const colorIndex = (newChoices.length + index) % COLORS.length;
        newChoices.push({
            name: name,
            color: COLORS[colorIndex]
        });
    });
    
    // Sort alphabetically (optional - remove if you want to preserve order)
    newChoices.sort((a, b) => a.name.localeCompare(b.name));
    
    try {
        await field.updateOptionsAsync({
            choices: newChoices
        });
        
        console.log("✓ Field options updated successfully!");
        
        // ============================================================================
        // DISPLAY RESULTS
        // ============================================================================
        
        let summaryMarkdown = "## ✅ Template Sync Complete\n\n";
        
        summaryMarkdown += `**Total Templates:** ${newChoices.length}\n\n`;
        
        if (newTemplates.length > 0) {
            summaryMarkdown += `### ➕ Added (${newTemplates.length})\n`;
            newTemplates.forEach(name => {
                const desc = templates[name].substring(0, 80);
                summaryMarkdown += `- **${name}**\n  ${desc}${templates[name].length > 80 ? '...' : ''}\n\n`;
            });
        }
        
        if (obsoleteTemplates.length > 0) {
            summaryMarkdown += `### ➖ Removed (${obsoleteTemplates.length})\n`;
            obsoleteTemplates.forEach(name => {
                summaryMarkdown += `- ~~${name}~~\n`;
            });
            summaryMarkdown += "\n";
        }
        
        summaryMarkdown += "### 📋 All Available Templates\n";
        newChoices.forEach((choice, index) => {
            const desc = templates[choice.name].substring(0, 60);
            summaryMarkdown += `${index + 1}. **${choice.name}**\n   ${desc}${templates[choice.name].length > 60 ? '...' : ''}\n\n`;
        });
        
        summaryMarkdown += "\n---\n\n";
        summaryMarkdown += "*You can now select these templates in the Template Type field when generating emails.*";
        
        output.markdown(summaryMarkdown);
        
    } catch (error) {
        console.error("❌ Error updating field options:", error);
        throw new Error(`Failed to update field: ${error.message}\n\nYou may need to:\n1. Check table/field permissions\n2. Verify you're not using this field in views with filters\n3. Try removing the field and recreating it`);
    }
}

// ============================================================================
// SUMMARY STATISTICS
// ============================================================================

console.log("\n" + "=".repeat(60));
console.log("SYNC SUMMARY");
console.log("=".repeat(60));
console.log(`Total templates in Google Drive: ${templateNames.length}`);
console.log(`Total options in Airtable field: ${newTemplates.length === 0 && obsoleteTemplates.length === 0 ? currentOptions.length : newChoices.length}`);
console.log(`Added: ${newTemplates.length}`);
console.log(`Removed: ${obsoleteTemplates.length}`);
console.log(`Unchanged: ${unchangedTemplates.length}`);
console.log("=".repeat(60));
