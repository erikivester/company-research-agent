import {
    initializeBlock,
    useBase,
    useRecords,
    useGlobalConfig,
    useLoadable,
    useWatchable,
    Box,
    Button,
    FormField,
    Input,
    Text,
    Heading,
    Select,
    Icon,
    Loader,
    FieldPickerSynced,
    TablePickerSynced,
    useSettingsButton,
} from '@airtable/blocks/ui';
import React, { useState, useEffect } from 'react';
import './style.css';

// Configuration keys for global config
const SETTINGS_KEYS = {
    API_ENDPOINT: 'apiEndpoint',
    TABLE_ID: 'tableId',
    CONTACT_NAME_FIELD_ID: 'contactNameFieldId',
    COMPANY_NAME_FIELD_ID: 'companyNameFieldId',
    CONTACT_TITLE_FIELD_ID: 'contactTitleFieldId',
    SUMMARY_FIELD_ID: 'summaryFieldId',
    ANGLE_FIELD_ID: 'angleFieldId',
    NOTE_FIELD_ID: 'noteFieldId',
    RESEARCH_FOLDER_FIELD_ID: 'researchFolderFieldId',
    DRAFT_FIELD_ID: 'draftFieldId',
    TEMPLATE_FIELD_ID: 'templateFieldId',
};

// Default API endpoint
const DEFAULT_API_ENDPOINT = 'https://futuramic-nonglandulous-senaida.ngrok-free.dev';

function EmailGeneratorApp() {
    const base = useBase();
    const globalConfig = useGlobalConfig();
    
    // State for settings panel
    const [isShowingSettings, setIsShowingSettings] = useState(false);
    useSettingsButton(() => {
        setIsShowingSettings(!isShowingSettings);
    });

    // Get configuration
    const apiEndpoint = globalConfig.get(SETTINGS_KEYS.API_ENDPOINT) || DEFAULT_API_ENDPOINT;
    const tableId = globalConfig.get(SETTINGS_KEYS.TABLE_ID);
    const table = base.getTableByIdIfExists(tableId);

    // Show settings if not configured
    if (isShowingSettings || !tableId) {
        return <SettingsPanel globalConfig={globalConfig} base={base} />;
    }

    if (!table) {
        return (
            <Box padding={3}>
                <Text textColor="red">Selected table no longer exists. Please reconfigure.</Text>
            </Box>
        );
    }

    return <MainPanel table={table} globalConfig={globalConfig} apiEndpoint={apiEndpoint} />;
}

function SettingsPanel({ globalConfig, base }) {
    const [apiEndpoint, setApiEndpoint] = useState(
        globalConfig.get(SETTINGS_KEYS.API_ENDPOINT) || DEFAULT_API_ENDPOINT
    );

    const tableId = globalConfig.get(SETTINGS_KEYS.TABLE_ID);
    const table = base.getTableByIdIfExists(tableId);
    
    const canSetConfig = globalConfig.hasPermissionToSet();

    const handleSaveEndpoint = async () => {
        if (canSetConfig) {
            await globalConfig.setAsync(SETTINGS_KEYS.API_ENDPOINT, apiEndpoint);
        }
    };

    return (
        <Box padding={3}>
            <Heading size="large">⚙️ Settings</Heading>
            {!canSetConfig && (
                <Box padding={2} backgroundColor="yellowLight2" borderRadius="default" marginTop={2}>
                    <Text textColor="orange">⚠️ You need creator/owner permissions to change settings</Text>
                </Box>
            )}
            <Box marginTop={3}>
                <FormField label="API Endpoint" description="Your FastAPI server URL (ngrok or production)">
                    <Input
                        value={apiEndpoint}
                        onChange={(e) => setApiEndpoint(e.target.value)}
                        placeholder="https://your-url.ngrok-free.dev"
                        disabled={!canSetConfig}
                    />
                    <Button
                        marginTop={2}
                        variant="primary"
                        onClick={handleSaveEndpoint}
                        disabled={!canSetConfig}
                    >
                        Save API Endpoint
                    </Button>
                </FormField>

                <FormField label="Table" description="Select the table with your prospect records" marginTop={3}>
                    <TablePickerSynced globalConfigKey={SETTINGS_KEYS.TABLE_ID} />
                </FormField>

                {table && (
                    <>
                        <Heading size="small" marginTop={3}>Field Mappings</Heading>
                        
                        <FormField label="Contact Name Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.CONTACT_NAME_FIELD_ID} />
                        </FormField>

                        <FormField label="Company Name Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.COMPANY_NAME_FIELD_ID} />
                        </FormField>

                        <FormField label="Contact Title Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.CONTACT_TITLE_FIELD_ID} />
                        </FormField>

                        <FormField label="Company Summary Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.SUMMARY_FIELD_ID} />
                        </FormField>

                        <FormField label="Angle for Outreach Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.ANGLE_FIELD_ID} />
                        </FormField>

                        <FormField label="Note Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.NOTE_FIELD_ID} />
                        </FormField>

                        <FormField label="Research Folder URL Field" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.RESEARCH_FOLDER_FIELD_ID} />
                        </FormField>

                        <FormField label="Email Draft Field (output)" marginTop={2}>
                            <FieldPickerSynced table={table} globalConfigKey={SETTINGS_KEYS.DRAFT_FIELD_ID} />
                        </FormField>

                        <FormField label="Template Type Field (optional)" marginTop={2}>
                            <FieldPickerSynced 
                                table={table} 
                                globalConfigKey={SETTINGS_KEYS.TEMPLATE_FIELD_ID}
                                placeholder="Select to enable dynamic templates"
                            />
                        </FormField>
                    </>
                )}

                <Box marginTop={3} padding={2} backgroundColor="lightGray1" borderRadius="default">
                    <Text textColor="gray">
                        💡 Tip: Set up all field mappings to enable email generation. The Template Type field is optional
                        - if not set, you'll select templates manually.
                    </Text>
                </Box>
            </Box>
        </Box>
    );
}

function MainPanel({ table, globalConfig, apiEndpoint }) {
    const [selectedRecordIds, setSelectedRecordIds] = useState([]);
    const [recordInput, setRecordInput] = useState('');
    const [templates, setTemplates] = useState({});
    const [selectedTemplate, setSelectedTemplate] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [isSyncing, setIsSyncing] = useState(false);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [progress, setProgress] = useState({ current: 0, total: 0 });
    
    // Load records from the table
    const records = useRecords(table);
    
    // Check permissions
    const draftFieldId = globalConfig.get(SETTINGS_KEYS.DRAFT_FIELD_ID);
    const draftField = draftFieldId ? table.getFieldByIdIfExists(draftFieldId) : null;
    const canUpdateRecords = draftField ? table.hasPermissionToUpdateRecord(undefined, draftField) : true; // Default to true if not configured yet

    // Fetch templates on mount
    useEffect(() => {
        fetchTemplates();
    }, [apiEndpoint]);

    const fetchTemplates = async () => {
        try {
            setError(null);
            const response = await fetch(`${apiEndpoint}/templates`, {
                headers: { 'ngrok-skip-browser-warning': 'true' }
            });
            
            if (!response.ok) {
                throw new Error(`Failed to fetch templates: ${response.status}`);
            }
            
            const data = await response.json();
            setTemplates(data);
            
            // Set first template as default
            const templateNames = Object.keys(data);
            if (templateNames.length > 0 && !selectedTemplate) {
                setSelectedTemplate(templateNames[0]);
            }
        } catch (err) {
            setError(`Template fetch failed: ${err.message}`);
        }
    };

    const syncTemplateField = async () => {
        const templateFieldId = globalConfig.get(SETTINGS_KEYS.TEMPLATE_FIELD_ID);
        if (!templateFieldId) {
            setError('Template field not configured. Go to Settings to set it up.');
            return;
        }

        setIsSyncing(true);
        setError(null);
        
        try {
            const field = table.getFieldById(templateFieldId);
            
            if (field.type !== 'singleSelect') {
                throw new Error('Template field must be a Single Select field');
            }

            const templateNames = Object.keys(templates);
            const currentOptions = field.options.choices;
            const currentNames = currentOptions.map(opt => opt.name);

            // Calculate changes
            const newTemplates = templateNames.filter(name => !currentNames.includes(name));
            const obsoleteTemplates = currentNames.filter(name => !templateNames.includes(name));

            if (newTemplates.length === 0 && obsoleteTemplates.length === 0) {
                setSuccess('✓ Template options already in sync!');
                return;
            }

            // Build new choices
            const colors = [
                'blueLight2', 'cyanLight2', 'tealLight2', 'greenLight2', 'yellowLight2',
                'orangeLight2', 'redLight2', 'pinkLight2', 'purpleLight2', 'grayLight2'
            ];

            const newChoices = [];
            
            // Keep existing valid options
            currentOptions.forEach(option => {
                if (templateNames.includes(option.name)) {
                    newChoices.push({ name: option.name, color: option.color });
                }
            });
            
            // Add new templates
            newTemplates.forEach((name, index) => {
                const colorIndex = (newChoices.length + index) % colors.length;
                newChoices.push({ name: name, color: colors[colorIndex] });
            });
            
            // Sort alphabetically
            newChoices.sort((a, b) => a.name.localeCompare(b.name));

            // Update field
            await field.updateOptionsAsync({ choices: newChoices });

            setSuccess(`✓ Synced! Added ${newTemplates.length}, removed ${obsoleteTemplates.length}`);
        } catch (err) {
            setError(`Sync failed: ${err.message}`);
        } finally {
            setIsSyncing(false);
        }
    };

    const generateEmails = async () => {
        if (selectedRecordIds.length === 0) {
            setError('Please select at least one record');
            return;
        }

        if (!selectedTemplate && !globalConfig.get(SETTINGS_KEYS.TEMPLATE_FIELD_ID)) {
            setError('Please select a template');
            return;
        }

        setIsLoading(true);
        setError(null);
        setSuccess(null);
        setProgress({ current: 0, total: selectedRecordIds.length });

        const contactNameFieldId = globalConfig.get(SETTINGS_KEYS.CONTACT_NAME_FIELD_ID);
        const companyNameFieldId = globalConfig.get(SETTINGS_KEYS.COMPANY_NAME_FIELD_ID);
        const contactTitleFieldId = globalConfig.get(SETTINGS_KEYS.CONTACT_TITLE_FIELD_ID);
        const summaryFieldId = globalConfig.get(SETTINGS_KEYS.SUMMARY_FIELD_ID);
        const angleFieldId = globalConfig.get(SETTINGS_KEYS.ANGLE_FIELD_ID);
        const noteFieldId = globalConfig.get(SETTINGS_KEYS.NOTE_FIELD_ID);
        const researchFolderFieldId = globalConfig.get(SETTINGS_KEYS.RESEARCH_FOLDER_FIELD_ID);
        const draftFieldId = globalConfig.get(SETTINGS_KEYS.DRAFT_FIELD_ID);
        const templateFieldId = globalConfig.get(SETTINGS_KEYS.TEMPLATE_FIELD_ID);

        let successCount = 0;
        let errorCount = 0;

        for (let i = 0; i < selectedRecordIds.length; i++) {
            const recordId = selectedRecordIds[i];
            const record = records.find(r => r.id === recordId);
            
            if (!record) continue;

            setProgress({ current: i + 1, total: selectedRecordIds.length });

            try {
                // Get field values
                const contactName = record.getCellValueAsString(contactNameFieldId) || '';
                const companyName = record.getCellValueAsString(companyNameFieldId) || '';
                const contactTitle = record.getCellValueAsString(contactTitleFieldId) || '';
                const summary = record.getCellValueAsString(summaryFieldId) || '';
                const angle = record.getCellValueAsString(angleFieldId) || '';
                const note = record.getCellValueAsString(noteFieldId) || '';
                
                let researchFolder = record.getCellValueAsString(researchFolderFieldId) || '';
                if (!researchFolder) {
                    researchFolder = 'https://drive.google.com/drive/folders/1lTGhNVVzG4cj_USBew3yuAPMmp0_cbC3';
                }

                // Determine template
                let templateToUse = selectedTemplate;
                if (templateFieldId) {
                    const templateCell = record.getCellValue(templateFieldId);
                    if (templateCell && templateCell.name) {
                        templateToUse = templateCell.name;
                    }
                }

                // Call API
                const response = await fetch(`${apiEndpoint}/generate-outreach`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'ngrok-skip-browser-warning': 'true'
                    },
                    body: JSON.stringify({
                        template_type: templateToUse,
                        contact_name: contactName,
                        airtable_context: {
                            name: companyName,
                            title: contactTitle,
                            summary: summary,
                            angle_for_outreach: angle,
                            note: note
                        },
                        google_drive_folder_url: researchFolder
                    })
                });

                if (!response.ok) {
                    throw new Error(`API error: ${response.status}`);
                }

                const result = await response.json();
                
                // Update record with generated email
                await table.updateRecordAsync(record.id, {
                    [draftFieldId]: result.email_text
                });

                successCount++;
            } catch (err) {
                console.error(`Error for record ${record.id}:`, err);
                errorCount++;
            }
        }

        setIsLoading(false);
        if (errorCount === 0) {
            setSuccess(`✅ Generated ${successCount} email(s) successfully!`);
        } else {
            setError(`⚠️ Generated ${successCount} email(s), ${errorCount} failed`);
        }
        setSelectedRecordIds([]);
    };

    const templateOptions = Object.keys(templates).map(name => ({
        value: name,
        label: name
    }));

    const hasTemplateField = !!globalConfig.get(SETTINGS_KEYS.TEMPLATE_FIELD_ID);

    return (
        <Box padding={3}>
            <Box display="flex" alignItems="center" justifyContent="space-between" marginBottom={3}>
                <Heading size="large">✉️ AI Email Generator</Heading>
                <Button
                    size="small"
                    icon="cog"
                    variant="secondary"
                    onClick={() => window.location.reload()}
                >
                    Settings
                </Button>
            </Box>
            
            {/* Permission Warning */}
            {draftField && !canUpdateRecords && (
                <Box padding={2} backgroundColor="yellowLight2" borderRadius="default" marginBottom={2}>
                    <Text textColor="orange">
                        ⚠️ You need edit permissions to update records. 
                        You can still configure settings but won't be able to generate emails.
                    </Text>
                </Box>
            )}

            {/* Status Messages */}
            {error && (
                <Box padding={2} backgroundColor="redLight2" borderRadius="default" marginBottom={2}>
                    <Text textColor="red">{error}</Text>
                </Box>
            )}
            {success && (
                <Box padding={2} backgroundColor="greenLight2" borderRadius="default" marginBottom={2}>
                    <Text textColor="green">{success}</Text>
                </Box>
            )}

            {/* Templates Section */}
            <Box marginBottom={3}>
                <Box display="flex" alignItems="center" justifyContent="space-between" marginBottom={2}>
                    <Heading size="small">� Templates</Heading>
                    <Box display="flex" gap={1}>
                        <Button
                            size="small"
                            icon="reload"
                            onClick={fetchTemplates}
                            disabled={isLoading}
                        >
                            Refresh
                        </Button>
                        {hasTemplateField && (
                            <Button
                                size="small"
                                icon="sync"
                                variant="primary"
                                onClick={syncTemplateField}
                                disabled={isSyncing || isLoading}
                            >
                                {isSyncing ? 'Syncing...' : 'Sync Field'}
                            </Button>
                        )}
                    </Box>
                </Box>

                {Object.keys(templates).length === 0 ? (
                    <Box padding={2} backgroundColor="lightGray1" borderRadius="default">
                        <Text textColor="gray">No templates found. Check API connection.</Text>
                    </Box>
                ) : (
                    <>
                        <Text textColor="gray" marginBottom={2}>
                            {Object.keys(templates).length} template(s) available
                        </Text>
                        {!hasTemplateField && (
                            <FormField label="Select template for all records">
                                <Select
                                    options={templateOptions}
                                    value={selectedTemplate}
                                    onChange={(newValue) => setSelectedTemplate(newValue)}
                                    width="100%"
                                />
                            </FormField>
                        )}
                        {hasTemplateField && (
                            <Box padding={2} backgroundColor="cyanLight2" borderRadius="default">
                                <Text>
                                    <Icon name="info" marginRight={1} />
                                    Using template from each record's Template Type field
                                </Text>
                            </Box>
                        )}
                    </>
                )}
            </Box>

            {/* Record Selection */}
            <Box marginBottom={3}>
                <Heading size="small" marginBottom={2}>👥 Select Records</Heading>
                <Box padding={3} border="default" borderRadius="default" backgroundColor="lightGray1">
                    <Text marginBottom={2}>
                        <Icon name="info" marginRight={1} />
                        Select records in the Airtable table, then use the Record IDs below:
                    </Text>
                    <FormField label="Record IDs (comma-separated)" marginBottom={2}>
                        <Input
                            value={recordInput}
                            onChange={(e) => setRecordInput(e.target.value)}
                            placeholder="rec123abc, rec456def, rec789ghi"
                        />
                    </FormField>
                    <Button
                        size="small"
                        onClick={() => {
                            const ids = recordInput.split(',').map(id => id.trim()).filter(Boolean);
                            setSelectedRecordIds(ids);
                        }}
                    >
                        Set Record IDs
                    </Button>
                </Box>
                <Box display="flex" justifyContent="space-between" alignItems="center" marginTop={2}>
                    <Text textColor="gray">
                        {selectedRecordIds.length} record(s) selected
                    </Text>
                    {selectedRecordIds.length > 0 && (
                        <Button
                            size="small"
                            onClick={() => {
                                setSelectedRecordIds([]);
                                setRecordInput('');
                            }}
                        >
                            Clear selection
                        </Button>
                    )}
                </Box>
                {selectedRecordIds.length > 0 && (
                    <Box marginTop={2} padding={2} backgroundColor="cyanLight2" borderRadius="default">
                        <Text size="small">
                            Selected IDs: {selectedRecordIds.join(', ')}
                        </Text>
                    </Box>
                )}
            </Box>

            {/* Generate Button */}
            <Box marginTop={3}>
                <Button
                    variant="primary"
                    size="large"
                    icon="premium"
                    onClick={generateEmails}
                    disabled={isLoading || selectedRecordIds.length === 0 || !canUpdateRecords}
                    width="100%"
                >
                    {isLoading 
                        ? `Generating... (${progress.current}/${progress.total})` 
                        : `Generate Email${selectedRecordIds.length > 1 ? 's' : ''}`
                    }
                </Button>
            </Box>

            {isLoading && (
                <Box display="flex" justifyContent="center" marginTop={2}>
                    <Loader />
                </Box>
            )}

            {/* Info */}
            <Box marginTop={3} padding={2} backgroundColor="lightGray1" borderRadius="default">
                <Text textColor="gray" size="small">
                    💡 Select one or more records, choose a template, and click Generate to create 
                    personalized emails using AI with your research context from Google Drive.
                </Text>
            </Box>
        </Box>
    );
}

initializeBlock(() => <EmailGeneratorApp />);
