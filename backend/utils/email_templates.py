"""
Dynamic template management for email outreach.
"""

import os
import json
from typing import Dict, List, Optional
from dataclasses import dataclass
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

logger = logging.getLogger(__name__)

@dataclass
class EmailTemplate:
    """Represents an email template from Google Drive."""
    id: str           # Google Drive file ID
    name: str         # Original filename
    type: str         # Template type (derived from filename)
    description: str  # Description from the first line of the template

class EmailTemplateManager:
    """Manages dynamic fetching and caching of email templates from Google Drive."""
    _instance = None
    
    def __new__(cls, folder_id: str = "1h_U3DyDXP1VX6E999zRlti_-xLeRkWOW"):
        if cls._instance is None:
            cls._instance = super(EmailTemplateManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, folder_id: str = "1h_U3DyDXP1VX6E999zRlti_-xLeRkWOW"):
        """
        Initialize the template manager.
        
        Args:
            folder_id: The Google Drive folder ID containing email templates
        """
        if not self._initialized:
            self.folder_id = folder_id
            self.templates: Dict[str, EmailTemplate] = {}
            self.drive_service = None
            self._initialized = True
    
    async def ensure_drive_service(self):
        """Lazily set up the Google Drive service when needed."""
        if self.drive_service is not None:
            return

        import asyncio
        credentials_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
        if not credentials_json:
            logger.warning("GDRIVE_CREDENTIALS_JSON not set, Drive features will be unavailable")
            return
                
        try:
            # Run the synchronous Drive API initialization in a thread pool
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._initialize_drive_service)
            if self.drive_service:
                logger.info("Google Drive service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Drive service: {e}")
            self.drive_service = None
            raise

    def _initialize_drive_service(self):
        """Internal method to initialize the Drive service synchronously."""
        credentials_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
        if not credentials_json:
            raise ValueError("GDRIVE_CREDENTIALS_JSON not set")
            
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        self.drive_service = build('drive', 'v3', credentials=credentials)

    def _extract_template_type(self, filename: str) -> str:
        """Extract template type from filename."""
        # Remove extension and convert to uppercase for consistency
        name = os.path.splitext(filename)[0].upper()
        # Replace spaces and hyphens with underscores
        return name.replace(" ", "_").replace("-", "_")

    def _extract_description(self, content: str) -> str:
        """Extract description from template content."""
        # Get first non-empty line as description
        lines = content.strip().split("\n")
        for line in lines:
            line = line.strip()
            if line and not line.startswith("#"):
                return line
        return "No description available"

    async def refresh_templates(self) -> None:
        """
        Fetch and cache all templates from the Google Drive folder.
        """
        try:
            await self.ensure_drive_service()
            if not self.drive_service:
                logger.warning("Drive service not available, using empty template list")
                self.templates = {}
                return

            # Run the Drive API calls in a thread pool
            import asyncio
            loop = asyncio.get_running_loop()
            
            # List all files in the templates folder
            query = f"'{self.folder_id}' in parents and trashed = false"
            logger.info(f"Searching for files in folder: {self.folder_id}")
            results = await loop.run_in_executor(None, 
                lambda: self.drive_service.files().list(
                    q=query,
                    spaces='drive',
                    fields='files(id, name, mimeType)',
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
            )
            
            files = results.get('files', [])
            logger.info(f"Found {len(files)} files in folder")
            for file in files:
                logger.info(f"Found file: {file['name']} (type: {file['mimeType']}, id: {file['id']})")

            new_templates = {}
            
            for file in results.get('files', []):
                try:
                    # Accept Google Docs and text-based files
                    mime_type = file['mimeType'].lower()
                    acceptable_types = [
                        'text/plain',
                        'text/markdown',
                        'application/json',
                        'text/html',
                        'application/x-javascript',
                        'application/javascript',
                        'text/javascript',
                        'application/vnd.google-apps.document'  # Google Docs
                    ]
                    
                    is_acceptable = mime_type.startswith('text/') or mime_type in acceptable_types
                    if not is_acceptable:
                        logger.debug(f"Skipping non-text file: {file['name']} (type: {mime_type})")
                        continue

                    logger.info(f"Processing file: {file['name']} (type: {mime_type})")
                    
                    # Handle Google Docs differently from regular files
                    if mime_type == 'application/vnd.google-apps.document':
                        content = await loop.run_in_executor(None,
                            lambda: self.drive_service.files().export(
                                fileId=file['id'],
                                mimeType='text/plain'
                            ).execute().decode('utf-8')
                        )
                    else:
                        # Regular file download
                        content = await loop.run_in_executor(None,
                            lambda: self.drive_service.files().get_media(
                                fileId=file['id']
                            ).execute().decode('utf-8')
                        )
                    
                    try:
                        template_type = self._extract_template_type(file['name'])
                        description = self._extract_description(content)
                        
                        new_templates[template_type] = EmailTemplate(
                            id=file['id'],
                            name=file['name'],
                            type=template_type,
                            description=description
                        )
                        logger.info(f"Successfully loaded template: {template_type} from {file['name']}")
                    except Exception as template_error:
                        logger.error(f"Error processing template metadata for {file['name']}: {template_error}")
                    logger.debug(f"Loaded template: {template_type} from {file['name']}")
                    
                except Exception as e:
                    logger.error(f"Error processing template {file['name']}: {e}")
                    continue

            self.templates = new_templates
            logger.info(f"Refreshed {len(self.templates)} templates from Drive folder")
            
        except HttpError as error:
            logger.error(f"Error accessing templates folder: {error}")
            self.templates = {}
        except Exception as e:
            logger.error(f"Unexpected error refreshing templates: {e}")
            self.templates = {}

    def list_templates(self) -> Dict[str, str]:
        """
        Get a dictionary of available templates and their descriptions.
        If there are duplicates, keeps only the most recent version.
        
        Returns:
            Dictionary mapping template types to their descriptions
        """
        # Remove duplicates by keeping only one template per type
        unique_templates = {}
        for template in self.templates.values():
            if template.type not in unique_templates or template.id > unique_templates[template.type].id:
                unique_templates[template.type] = template
        
        return {
            template.type: template.description
            for template in unique_templates.values()
        }

    def get_template_id(self, template_type: str) -> Optional[str]:
        """
        Get the Google Drive file ID for a template type.
        
        Args:
            template_type: The type of template to find
            
        Returns:
            Google Drive file ID if found, None otherwise
        """
        template = self.templates.get(template_type.upper())
        return template.id if template else None

# Lazy-loaded singleton instance getter
def get_template_manager() -> EmailTemplateManager:
    """Get the global template manager instance."""
    return EmailTemplateManager()