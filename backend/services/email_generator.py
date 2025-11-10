"""
Service class for generating personalized outreach emails based on templates and research context.
"""
from typing import Dict, List, Optional, Tuple
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

logger = logging.getLogger(__name__)

class EmailGeneratorService:
    _instance = None

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

        credentials_path = self.credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set, Drive features will be unavailable")
            return

        try:
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

            # Get the template content asynchronously
            loop = asyncio.get_running_loop()
            content = await loop.run_in_executor(None,
                lambda: self.drive_service.files().get_media(fileId=template_id).execute()
            )
            template = template_manager.templates.get(template_type.upper())
            return template.name, content.decode('utf-8')

        except HttpError as error:
            logger.error(f"Error accessing template: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching template: {e}")
            return None

    async def fetch_research_context(self, folder_url: str) -> Dict[str, str]:
        """
        Fetch and parse research files from a Google Drive folder.
        
        Args:
            folder_url: URL to the Google Drive folder containing research files
            
        Returns:
            Dictionary containing parsed content from research files
        """
        try:
            # Extract folder ID from URL
            folder_id = folder_url.split('/')[-1]
            
            # List all files in the folder
            query = f"'{folder_id}' in parents and trashed = false"
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, mimeType)'
            ).execute()

            research_data = {}
            
            for file in results.get('files', []):
                try:
                    # Download and parse content based on file type
                    content = self.drive_service.files().get_media(fileId=file['id']).execute()
                    
                    if file['name'].endswith('.json'):
                        research_data[file['name']] = json.loads(content)
                    elif file['name'].endswith(('.txt', '.md')):
                        research_data[file['name']] = content.decode('utf-8')
                    elif file['name'].endswith('.pdf'):
                        # For now, skip PDFs as we'd need more complex parsing
                        continue
                        
                except Exception as e:
                    logger.error(f"Error processing file {file['name']}: {e}")
                    continue

            return research_data

        except HttpError as error:
            logger.error(f"Error accessing research folder: {error}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching research context: {e}")
            return {}

    def _prepare_research_summary(self, research_context: Dict[str, str]) -> str:
        """
        Prepare a concise summary of research data for the LLM prompt.
        """
        summary_parts = []
        
        # Process research context files
        for filename, content in research_context.items():
            if isinstance(content, dict):
                # Handle JSON data
                if 'company_brief_data' in content:
                    # Extract key insights from company brief
                    summary_parts.append("Company Research Insights:")
                    for url, data in content['company_brief_data'].items():
                        if isinstance(data, dict):
                            if 'title' in data and 'content' in data:
                                summary_parts.append(f"- {data['title']}: {data['content'][:300]}...")
            else:
                # Handle text/markdown content
                preview = content[:500] + ("..." if len(content) > 500 else "")
                summary_parts.append(f"From {filename}:")
                summary_parts.append(preview)
        
        return "\n\n".join(summary_parts)

    async def generate_email(self, 
                           template_content: str,
                           research_context: Dict[str, str],
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
        """
        try:
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
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=2000
            )
            
            return response.choices[0].message.content.strip()

        except Exception as e:
            logger.error(f"Error generating email: {e}")
            return None