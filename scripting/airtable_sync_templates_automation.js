/**
 * Airtable Template Sync Script (Automation Version)
 * 
 * This version is designed to run in Airtable Automations.
 * It syncs the Template Type field options with templates from Google Drive.
 * 
 * SETUP:
 * 1. Create a new automation
 * 2. Trigger: "At a scheduled time" (e.g., daily at 9 AM)
 * 3. Action: "Run a script" (this script)
 * 4. No input variables needed
 * 
 * OPTIONAL INPUT VARIABLES:
 * - apiEndpoint: Override the default API endpoint
 * - tableName: Override the default table name
 * - fieldName: Override the default field name
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

const API_BASE_URL = "https://futuramic-nonglandulous-senaida.ngrok-free.dev";
const DEFAULT_TABLE_NAME = "Corporate Prospects";
const DEFAULT_FIELD_NAME = "Template Type";

// Get configuration from input or use defaults
const apiEndpoint = input.config().apiEndpoint || API_BASE_URL;
const tableName = input.config().tableName || DEFAULT_TABLE_NAME;
const fieldName = input.config().fieldName || DEFAULT_FIELD_NAME;

const COLORS = [
    "blueLight2", "cyanLight2", "tealLight2", "greenLight2", "yellowLight2",
    "orangeLight2", "redLight2", "pinkLight2", "purpleLight2", "grayLight2"
];

// ============================================================================
// FETCH TEMPLATES FROM API
// ============================================================================

console.log(`Fetching templates from ${apiEndpoint}/templates`);

let templatesResponse;
try {
    templatesResponse = await fetch(`${apiEndpoint}/templates`, {
        method: "GET",
        headers: { "ngrok-skip-browser-warning": "true" }
    });
} catch (error) {
    throw new Error(`API connection failed: ${error.message}`);
}

if (!templatesResponse.ok) {
    throw new Error(`API error ${templatesResponse.status}: ${await templatesResponse.text()}`);
}

const templates = await templatesResponse.json();
const templateNames = Object.keys(templates);

console.log(`Found ${templateNames.length} templates from API`);

if (templateNames.length === 0) {
    throw new Error("No templates found in Google Drive");
}

// ============================================================================
// UPDATE FIELD OPTIONS
// ============================================================================

const table = base.getTable(tableName);
const field = table.getField(fieldName);

if (field.type !== "singleSelect") {
    throw new Error(`Field "${fieldName}" must be Single Select (current: ${field.type})`);
}

const currentOptions = field.options.choices;
const currentNames = currentOptions.map(opt => opt.name);

// Calculate changes
const newTemplates = templateNames.filter(name => !currentNames.includes(name));
const obsoleteTemplates = currentNames.filter(name => !templateNames.includes(name));

console.log(`Unchanged: ${templateNames.filter(n => currentNames.includes(n)).length}`);
console.log(`To add: ${newTemplates.length}`);
console.log(`To remove: ${obsoleteTemplates.length}`);

if (newTemplates.length === 0 && obsoleteTemplates.length === 0) {
    console.log("✓ Options already in sync");
    output.set("sync_needed", false);
    output.set("templates_count", templateNames.length);
} else {
    // Build new choices
    const newChoices = [];
    
    // Keep existing valid options
    currentOptions.forEach(option => {
        if (templateNames.includes(option.name)) {
            newChoices.push({ name: option.name, color: option.color });
        }
    });
    
    // Add new templates
    newTemplates.forEach((name, index) => {
        const colorIndex = (newChoices.length + index) % COLORS.length;
        newChoices.push({ name: name, color: COLORS[colorIndex] });
    });
    
    // Sort alphabetically
    newChoices.sort((a, b) => a.name.localeCompare(b.name));
    
    // Update field
    await field.updateOptionsAsync({ choices: newChoices });
    
    console.log(`✓ Updated field with ${newChoices.length} options`);
    
    output.set("sync_needed", true);
    output.set("templates_count", newChoices.length);
    output.set("added_count", newTemplates.length);
    output.set("removed_count", obsoleteTemplates.length);
    output.set("added_templates", newTemplates.join(", "));
    output.set("removed_templates", obsoleteTemplates.join(", "));
}

console.log("Template sync completed successfully");
