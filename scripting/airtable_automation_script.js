/**
 * Airtable Automation Script for Email Generation
 * 
 * This script is designed to run in Airtable Automations (not Scripting Extension).
 * 
 * SETUP INSTRUCTIONS:
 * 1. Create a new automation in Airtable
 * 2. Choose your trigger (e.g., "When record matches conditions", "When button clicked")
 * 3. Add a "Run a script" action
 * 4. Copy this code into the script action
 * 5. Configure input variables (see below)
 * 
 * REQUIRED INPUT VARIABLES:
 * - recordId (string): The record ID from the trigger
 * - contactName (string): Name of the contact person
 * 
 * OPTIONAL INPUT VARIABLES:
 * - templateType (string): Override template selection (if not provided, reads from record field)
 * - apiEndpoint (string): Override the default API endpoint (default: your ngrok URL)
 * - useTemplateField (boolean): If true, reads template from "Template Type" field in record
 * - defaultTemplate (string): Fallback template if none specified (default: "STANDARD_INTRO")
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

const API_BASE_URL = "https://futuramic-nonglandulous-senaida.ngrok-free.dev";
const DEFAULT_TEMPLATE = "STANDARD_INTRO";
const TEMPLATE_FIELD_NAME = "Template Type"; // Name of the field in your Airtable base

// ============================================================================
// GET INPUT VARIABLES
// ============================================================================

// Get the input variables configured in the automation
const recordId = input.config().recordId;
const contactName = input.config().contactName;
const apiEndpoint = input.config().apiEndpoint || API_BASE_URL;

// Template selection priority:
// 1. Explicit templateType from input variable
// 2. Template Type field from the record
// 3. Default template
const inputTemplateType = input.config().templateType;
const useTemplateField = input.config().useTemplateField !== false; // Default to true
const defaultTemplate = input.config().defaultTemplate || DEFAULT_TEMPLATE;

// Validate required inputs
if (!recordId) {
    throw new Error("Missing required input: recordId");
}
if (!contactName) {
    throw new Error("Missing required input: contactName");
}

// ============================================================================
// FETCH RECORD DATA
// ============================================================================

// Get the table (adjust table name if needed)
const table = base.getTable("Corporate Prospects");
const record = await table.selectRecordAsync(recordId);

if (!record) {
    throw new Error(`Record not found: ${recordId}`);
}

// Extract field values (adjust field names to match your base)
const companyName = record.getCellValue("Name") || "";
const contactTitle = record.getCellValue("Contact Title") || "";
const summary = record.getCellValue("Company Summary") || "";
const angleForOutreach = record.getCellValue("Angle for Outreach") || "";
const note = record.getCellValue("Note") || "";

// ============================================================================
// DYNAMIC TEMPLATE SELECTION
// ============================================================================

let templateType;

// Priority 1: Explicit input variable (highest priority)
if (inputTemplateType) {
    templateType = inputTemplateType;
    console.log(`Using template from input variable: ${templateType}`);
}
// Priority 2: Read from record field
else if (useTemplateField) {
    const templateFieldValue = record.getCellValue(TEMPLATE_FIELD_NAME);
    if (templateFieldValue) {
        // Handle both single select and text fields
        templateType = typeof templateFieldValue === 'object' && templateFieldValue.name 
            ? templateFieldValue.name 
            : String(templateFieldValue);
        console.log(`Using template from record field "${TEMPLATE_FIELD_NAME}": ${templateType}`);
    } else {
        templateType = defaultTemplate;
        console.log(`No template in field "${TEMPLATE_FIELD_NAME}", using default: ${templateType}`);
    }
}
// Priority 3: Use default
else {
    templateType = defaultTemplate;
    console.log(`Using default template: ${templateType}`);
}

// Validate template type was determined
if (!templateType) {
    throw new Error("Could not determine template type. Please set templateType input variable, add a Template Type field, or configure defaultTemplate.");
}

// ============================================================================
// GET RESEARCH FOLDER URL
// ============================================================================

// Get linked research folder URL
let researchFolderUrl = "";
const researchFolderField = record.getCellValue("Research Drive Folder");
if (researchFolderField) {
    // Handle linked record or URL field
    if (Array.isArray(researchFolderField) && researchFolderField.length > 0) {
        researchFolderUrl = researchFolderField[0].name || researchFolderField[0];
    } else if (typeof researchFolderField === "string") {
        researchFolderUrl = researchFolderField;
    }
}

// Default folder if none specified
if (!researchFolderUrl) {
    researchFolderUrl = "https://drive.google.com/drive/folders/1lTGhNVVzG4cj_USBew3yuAPMmp0_cbC3";
}

console.log("Processing record:", {
    recordId,
    companyName,
    contactName,
    templateType,
    researchFolderUrl
});

// ============================================================================
// CALL API TO GENERATE EMAIL
// ============================================================================

const requestPayload = {
    template_type: templateType,
    contact_name: contactName,
    airtable_context: {
        name: companyName,
        title: contactTitle,
        summary: summary,
        angle_for_outreach: angleForOutreach,
        note: note
    },
    google_drive_folder_url: researchFolderUrl
};

console.log("Calling API with payload:", JSON.stringify(requestPayload, null, 2));

let response;
try {
    response = await fetch(`${apiEndpoint}/generate-outreach`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "ngrok-skip-browser-warning": "true"
        },
        body: JSON.stringify(requestPayload)
    });
} catch (error) {
    throw new Error(`API request failed: ${error.message}`);
}

if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`API returned error ${response.status}: ${errorText}`);
}

const result = await response.json();
console.log("API response received:", {
    template_used: result.template_used,
    context_used: result.context_used,
    email_length: result.email_text?.length || 0
});

// ============================================================================
// UPDATE RECORD WITH GENERATED EMAIL
// ============================================================================

const emailDraft = result.email_text || "";

if (!emailDraft) {
    throw new Error("API returned empty email text");
}

// Update the record with the generated email draft
// Adjust the field name "Email Draft" to match your base
await table.updateRecordAsync(recordId, {
    "Email Draft": emailDraft
});

console.log(`✓ Successfully generated and saved email draft (${emailDraft.length} characters)`);

// ============================================================================
// OUTPUT SUMMARY
// ============================================================================

// Return summary data (visible in automation run logs)
output.set("success", true);
output.set("recordId", recordId);
output.set("companyName", companyName);
output.set("emailLength", emailDraft.length);
output.set("templateUsed", result.template_used);
output.set("contextUsed", result.context_used);

console.log("=".repeat(60));
console.log("EMAIL GENERATION COMPLETED");
console.log("=".repeat(60));
console.log(`Company: ${companyName}`);
console.log(`Contact: ${contactName}`);
console.log(`Template: ${result.template_used}`);
console.log(`Context Sources:`, result.context_used);
console.log(`Email Length: ${emailDraft.length} characters`);
console.log("=".repeat(60));
