"""
Service class for generating personalized outreach emails based on templates and research context.
"""
from typing import Dict, List, Optional, Tuple, Any
import logging
import os
import json
import asyncio
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from openai import AsyncOpenAI
import tiktoken
from backend.utils.email_templates import get_template_manager
from backend.utils.research_parser import ResearchFileParser
from backend.utils.exceptions import (
    EmailGeneratorError,
    TemplateNotFoundError,
    DriveServiceError,
    ResearchContextError,
    EmailGenerationError
)

logger = logging.getLogger(__name__)

class EmailGeneratorService:
    _instance = None
    _cache = {}  # Class-level cache for research context

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EmailGeneratorService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, credentials_path: str = None, model: str = "gpt-4-1106-preview", openai_api_key: str = None):
        """
        Initialize the EmailGenerator service.
        
        Args:
            credentials_path: Path to the Google service account credentials file.
                            If None, will try to use the GOOGLE_APPLICATION_CREDENTIALS env var.
            model: The OpenAI model to use for email generation
            openai_api_key: Optional OpenAI API key. If not provided, will use OPENAI_API_KEY env var.
            
        Raises:
            DriveServiceError: If credentials are missing or invalid
        """
        if self._initialized:
            return

        self.credentials_path = credentials_path
        self.drive_service = None
        
        # Initialize OpenAI client
        self.client = AsyncOpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY", "test-key-for-unit-tests"))
        self.model = model
        self.encoding = tiktoken.encoding_for_model(model)
        self._initialized = True

    def ensure_drive_service(self):
        """Lazily initialize the Google Drive service when needed."""
        if self.drive_service is not None:
            return

        # Try to get credentials from either file path or JSON string
        credentials_path = self.credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        credentials_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
        
        if not credentials_path and not credentials_json:
            logger.warning("No Google Drive credentials found (GOOGLE_APPLICATION_CREDENTIALS or GDRIVE_CREDENTIALS_JSON), Drive features will be unavailable")
            return

        try:
            if credentials_json:
                # Load credentials from JSON string
                import json
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=['https://www.googleapis.com/auth/drive.readonly']
                )
            else:
                # Load credentials from file
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/drive.readonly']
                )
            
            self.drive_service = build('drive', 'v3', credentials=credentials)
            logger.info("Google Drive service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Drive service: {e}")
            self.drive_service = None

    async def fetch_template(self, template_type: str) -> Optional[Tuple[str, str]]:
        """
        Fetch an email template from the shared Google Workspace folder.
        
        Args:
            template_type: The type of template to fetch (e.g., "CGF_METHANE_CALL")
            
        Returns:
            Tuple of (template_name, template_content) if found, None if not found
        """
        try:
            # Ensure Drive service is initialized
            self.ensure_drive_service()
            if not self.drive_service:
                logger.error("Drive service not available")
                return None

            # Get template manager and ensure templates are loaded
            template_manager = get_template_manager()
            await template_manager.refresh_templates()
            
            # Get template ID from manager
            template_id = template_manager.get_template_id(template_type)
            if not template_id:
                logger.error(f"Template not found: {template_type}")
                return None

            # Get template metadata to check file type
            template = template_manager.templates.get(template_type.upper())
            if not template:
                logger.error(f"Template metadata not found: {template_type}")
                return None

            # Get the template content asynchronously
            loop = asyncio.get_running_loop()
            
            # Check if it's a Google Doc (needs export) or a regular file (needs download)
            if 'google-apps' in template.name.lower() or template_id.startswith('1'):
                # Export Google Docs as plain text
                content = await loop.run_in_executor(None,
                    lambda: self.drive_service.files().export(
                        fileId=template_id,
                        mimeType='text/plain'
                    ).execute()
                )
            else:
                # Download regular files
                content = await loop.run_in_executor(None,
                    lambda: self.drive_service.files().get_media(fileId=template_id).execute()
                )
            
            return template.name, content.decode('utf-8') if isinstance(content, bytes) else content

        except HttpError as error:
            logger.error(f"Error accessing template: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching template: {e}")
            return None

    async def fetch_research_context(self, folder_url: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Fetch and parse research files from a Google Drive folder.
        
        Args:
            folder_url: URL to the Google Drive folder containing research files
            use_cache: Whether to use cached research context if available
            
        Returns:
            Dictionary containing parsed content from research files
            
        Raises:
            ResearchContextError: If there are issues fetching or parsing research
            DriveServiceError: If there are issues accessing Drive
        """
        try:
            # Check cache first if enabled
            if use_cache and folder_url in self._cache:
                logger.info(f"Using cached research context for folder: {folder_url}")
                return self._cache[folder_url]

            # Extract folder ID from URL
            folder_id = folder_url.split('/')[-1]
            
            # Ensure Drive service is initialized
            self.ensure_drive_service()
            if not self.drive_service:
                logger.warning("Drive service not available for research context")
                return {}
            
            # List all files in the folder (including Shared Drive files)
            query = f"'{folder_id}' in parents and trashed = false"
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, mimeType)',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            files = results.get('files', [])
            logger.info(f"Found {len(files)} files in research folder: {folder_id}")
            
            research_data = {}
            
            for file in files:
                try:
                    logger.info(f"Processing research file: {file['name']} ({file['mimeType']})")
                    
                    # Skip PDFs and binary files for now - they need special handling
                    if file['mimeType'] == 'application/pdf':
                        logger.info(f"Skipping PDF file (not yet supported): {file['name']}")
                        continue
                    
                    # Download file content based on type
                    if 'google-apps' in file['mimeType']:
                        # Export Google Docs/Sheets as plain text
                        content = self.drive_service.files().export(
                            fileId=file['id'],
                            mimeType='text/plain'
                        ).execute()
                    else:
                        # Download regular files
                        content = self.drive_service.files().get_media(
                            fileId=file['id']
                        ).execute()
                    
                    # Decode content
                    if isinstance(content, bytes):
                        try:
                            content_str = content.decode('utf-8')
                        except UnicodeDecodeError:
                            # Try with a more forgiving encoding
                            content_str = content.decode('utf-8', errors='ignore')
                    else:
                        content_str = content
                    
                    # Parse content based on file type using ResearchFileParser
                    parsed_content = ResearchFileParser.parse_file(
                        file_path=file['name'],
                        content=content_str
                    )
                    research_data[file['name']] = parsed_content
                    logger.info(f"Successfully processed: {file['name']}")
                        
                except Exception as e:
                    logger.error(f"Error processing file {file['name']}: {e}")
                    continue

            # Cache the results if cache is enabled
            if use_cache:
                self._cache[folder_url] = research_data

            return research_data

        except HttpError as error:
            raise DriveServiceError(f"Error accessing research folder: {error}")
        except Exception as e:
            if isinstance(e, DriveServiceError):
                raise
            raise ResearchContextError(f"Error fetching research context: {e}")

    def _prepare_research_summary(self, research_context: Dict[str, Any]) -> str:
        """
        Prepare a concise summary of research data for the LLM prompt.
        
        Args:
            research_context: Dictionary containing parsed research file contents
            
        Returns:
            Formatted summary string for the prompt
        """
        summary_parts = []
        
        for filename, file_data in research_context.items():
            content_type = file_data.get('type', 'unknown')
            content = file_data.get('content', {})
            
            if content_type == 'json':
                if isinstance(content, dict) and 'company_brief_data' in content:
                    summary_parts.append("Company Research Insights:")
                    for url, data in content['company_brief_data'].items():
                        if isinstance(data, dict):
                            if 'title' in data and 'content' in data:
                                summary_parts.append(f"- {data['title']}: {data['content'][:300]}...")
            
            elif content_type in ['text', 'markdown', 'pdf']:
                if isinstance(content, str):
                    preview = content[:500] + ("..." if len(content) > 500 else "")
                    summary_parts.append(f"From {filename}:")
                    summary_parts.append(preview)
        
        return "\n\n".join(summary_parts)

    async def generate_email(self, 
                           template_content: str,
                           research_context: Dict[str, Any],
                           airtable_context: Dict[str, str],
                           contact_name: str) -> str:
        """
        Generate a personalized email using AI by combining template, research, and Airtable context.
        
        Args:
            template_content: The base email template content
            research_context: Dictionary containing research file contents
            airtable_context: Dictionary containing fields from Airtable record
            contact_name: Name of the contact to email
            
        Returns:
            Generated email text
            
        Raises:
            EmailGenerationError: If there are issues generating the email
        """
        try:
            # Validate inputs
            if not template_content:
                raise EmailGenerationError("Template content is required")
            if not contact_name:
                raise EmailGenerationError("Contact name is required")
            if not airtable_context:
                raise EmailGenerationError("Airtable context is required")
            
            # Prepare research summary
            research_summary = self._prepare_research_summary(research_context)
            
            # Construct the prompt
            system_prompt = """You are an expert at writing highly personalized and compelling business outreach emails. 
Your task is to generate a professional email that follows the provided template structure while incorporating specific insights from research and strategic context.

Guidelines:
1. Maintain the core message and purpose from the template
2. Use research insights to create a strong, personalized opening hook
3. Integrate strategic talking points naturally into the email body
4. Keep the tone professional but conversational
5. Be specific and substantive - avoid generic statements
6. Ensure all claims are supported by the research context provided"""

            user_prompt = f"""Generate a personalized email using the following information:

1. RECIPIENT
Contact Name: {contact_name}
Title: {airtable_context.get('title', 'N/A')}
Company: {airtable_context.get('name', 'N/A')}

2. TEMPLATE STRUCTURE
{template_content}

3. CONTEXT & RESEARCH INSIGHTS
Company Summary: {airtable_context.get('summary', 'N/A')}
Strategic Angle: {airtable_context.get('angle_for_outreach', 'N/A')}
Additional Notes: {airtable_context.get('note', 'N/A')}

Research Context:
{research_summary}

Generate the complete email maintaining proper formatting and structure. The email should feel personal, well-researched, and strategically aligned with our outreach goals."""

            # Call the OpenAI API
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.7,
                    max_tokens=2000
                )
                
                generated_text = response.choices[0].message.content.strip()
                if not generated_text:
                    raise EmailGenerationError("OpenAI API returned empty response")
                    
                return generated_text

            except Exception as api_error:
                raise EmailGenerationError(f"OpenAI API error: {str(api_error)}")

        except Exception as e:
            if isinstance(e, EmailGenerationError):
                raise
            raise EmailGenerationError(f"Error generating email: {str(e)}")