"""
Service class for generating personalized outreach emails based on templates and research context.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

import tiktoken
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from openai import AsyncOpenAI

from backend.utils.email_templates import get_template_manager
from backend.utils.exceptions import (
    DriveServiceError,
    EmailGenerationError,
    ResearchContextError,
)
from backend.utils.research_parser import ResearchFileParser

logger = logging.getLogger(__name__)


class EmailGeneratorService:
    _instance = None
    _cache = {}  # Class-level cache for research context

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EmailGeneratorService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
        credentials_path: str = None,
        model: str = "gpt-4o",
        openai_api_key: str = None,
    ):
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
        self.client = AsyncOpenAI(
            api_key=openai_api_key
            or os.getenv("OPENAI_API_KEY", "test-key-for-unit-tests")
        )
        self.model = model
        self.encoding = tiktoken.encoding_for_model(model)
        self._initialized = True

    def ensure_drive_service(self):
        """Lazily initialize the Google Drive service when needed."""
        if self.drive_service is not None:
            return

        # Check multiple possible credential file locations
        credential_paths = [
            "/secrets/gdrive_credentials.json",  # Cloud Run secret mount
            "/app/gdrive_credentials.json",      # Docker volume mount
            "gdrive_credentials.json"            # Local development
        ]

        # First try to find a credentials file
        credentials_path = self.credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # If no explicit path from env, check standard locations
        if not credentials_path:
            for path in credential_paths:
                if os.path.exists(path):
                    credentials_path = path
                    break

        try:
            if credentials_path:
                # Load credentials from file (preferred method)
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=["https://www.googleapis.com/auth/drive.readonly"],
                )
                logger.info(f"Google Drive service initialized from file: {credentials_path}")
            else:
                # Fallback to environment variable (only if no file found)
                credentials_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
                if not credentials_json:
                    logger.warning(
                        f"No Google Drive credentials found. Checked paths: {credential_paths}, env vars: GOOGLE_APPLICATION_CREDENTIALS, GDRIVE_CREDENTIALS_JSON"
                    )
                    return

                import json
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=["https://www.googleapis.com/auth/drive.readonly"],
                )
                logger.info("Google Drive service initialized from GDRIVE_CREDENTIALS_JSON env var")

            self.drive_service = build("drive", "v3", credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to initialize Drive service: {e}", exc_info=True)
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
            if "google-apps" in template.name.lower() or template_id.startswith("1"):
                # Export Google Docs as plain text
                content = await loop.run_in_executor(
                    None,
                    lambda: self.drive_service.files()
                    .export(fileId=template_id, mimeType="text/plain")
                    .execute(),
                )
            else:
                # Download regular files
                content = await loop.run_in_executor(
                    None,
                    lambda: self.drive_service.files()
                    .get_media(fileId=template_id)
                    .execute(),
                )

            return template.name, (
                content.decode("utf-8") if isinstance(content, bytes) else content
            )

        except HttpError as error:
            logger.error(f"Error accessing template: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching template: {e}")
            return None

    async def fetch_research_context(
        self, folder_url: str, use_cache: bool = True
    ) -> Dict[str, Any]:
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
            folder_id = folder_url.split("/")[-1]

            # Ensure Drive service is initialized
            self.ensure_drive_service()
            if not self.drive_service:
                logger.warning("Drive service not available for research context")
                return {}

            # List all files in the folder (including Shared Drive files)
            query = f"'{folder_id}' in parents and trashed = false"
            results = (
                self.drive_service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, name, mimeType)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )

            files = results.get("files", [])
            logger.info(f"Found {len(files)} files in research folder: {folder_id}")

            research_data = {}

            for file in files:
                try:
                    logger.info(
                        f"Processing research file: {file['name']} ({file['mimeType']})"
                    )

                    # Skip PDFs and binary files for now - they need special handling
                    if file["mimeType"] == "application/pdf":
                        logger.info(
                            f"Skipping PDF file (not yet supported): {file['name']}"
                        )
                        continue

                    # Download file content based on type
                    if "google-apps" in file["mimeType"]:
                        # Export Google Docs/Sheets as plain text
                        content = (
                            self.drive_service.files()
                            .export(fileId=file["id"], mimeType="text/plain")
                            .execute()
                        )
                    else:
                        # Download regular files
                        content = (
                            self.drive_service.files()
                            .get_media(fileId=file["id"])
                            .execute()
                        )

                    # Decode content
                    if isinstance(content, bytes):
                        try:
                            content_str = content.decode("utf-8")
                        except UnicodeDecodeError:
                            # Try with a more forgiving encoding
                            content_str = content.decode("utf-8", errors="ignore")
                    else:
                        content_str = content

                    # Parse content based on file type using ResearchFileParser
                    parsed_content = ResearchFileParser.parse_file(
                        file_path=file["name"], content=content_str
                    )
                    research_data[file["name"]] = parsed_content
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
        Now focuses on markdown and text files only, no longer parsing JSON.

        Args:
            research_context: Dictionary containing parsed research file contents

        Returns:
            Formatted summary string for the prompt
        """
        summary_parts = []

        for filename, file_data in research_context.items():
            content_type = file_data.get("type", "unknown")
            content = file_data.get("content", {})

            # Only process text-based files (markdown, text, pdf)
            if content_type in ["text", "markdown", "pdf"]:
                if isinstance(content, str) and content.strip():
                    # Include more content for markdown files as they're typically well-structured
                    max_length = 2000 if content_type == "markdown" else 500
                    preview = content[:max_length] + ("..." if len(content) > max_length else "")
                    summary_parts.append(f"From {filename}:")
                    summary_parts.append(preview)

        return "\n\n".join(summary_parts) if summary_parts else "No additional research files found."

    async def generate_email(
        self,
        template_content: str,
        research_context: Dict[str, Any],
        airtable_context: Dict[str, str],
        contact_name: str,
    ) -> Tuple[str, str]:
        """
        Generate a personalized email using AI by combining template, research, and Airtable context.

        Args:
            template_content: The base email template content
            research_context: Dictionary containing research file contents
            airtable_context: Dictionary containing fields from Airtable record
            contact_name: Name of the contact to email

        Returns:
            Tuple of (email_text, subject_line)

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

            # Get markdown report from Airtable context (primary source)
            markdown_report = airtable_context.get('markdown_report', '')

            # Prepare research summary from Drive folder (secondary/supplementary)
            research_summary = self._prepare_research_summary(research_context)

            # Construct the prompt
            system_prompt = """You are an expert at writing concise, professional business outreach emails.
Your task is to generate an email that CLOSELY FOLLOWS the provided template structure and tone while incorporating specific insights.

CRITICAL GUIDELINES:
1. STICK TO THE TEMPLATE: Follow the template's structure, length, and tone precisely. Do NOT make it more elaborate or flowery.
2. USE DIRECT, PROFESSIONAL LANGUAGE: Avoid flowery or overly enthusiastic language. Keep it straightforward and business-appropriate.
3. PRIORITIZE THE STRATEGIC ANGLE: The "Strategic Angle for Outreach" is the MOST IMPORTANT input - it guides how you should position the outreach.
4. USE THE MARKDOWN REPORT: This is your primary source of detailed company intelligence. Reference specific facts from it.
5. BE SPECIFIC BUT BRIEF: Use concrete details but don't elaborate unnecessarily. One or two specific references are better than many generic ones.
6. MATCH THE TEMPLATE TONE: If the template is casual, be casual. If formal, be formal. Don't default to flowery language.
7. NO UNNECESSARY EMBELLISHMENT: Don't add extra compliments, praise, or enthusiasm that isn't in the template.
8. PRESERVE FORMATTING: Use double line breaks (\\n\\n) between paragraphs to maintain readability. Each paragraph should be separated by a blank line.

Important: You must return your response in this exact JSON format:
{
  "subject": "The email subject line here",
  "body": "The full email body here with \\n\\n between paragraphs"
}"""

            user_prompt = f"""Generate a personalized email using the following information:

==== RECIPIENT ====
Contact Name: {contact_name}
Title: {airtable_context.get('title', 'N/A')}
Company: {airtable_context.get('name', 'N/A')}

==== TEMPLATE (FOLLOW THIS STRUCTURE CLOSELY) ====
{template_content}

==== PRIMARY INPUTS (HIGHEST PRIORITY) ====

STRATEGIC ANGLE FOR OUTREACH (THIS GUIDES YOUR ENTIRE APPROACH):
{airtable_context.get('angle_for_outreach', 'N/A')}

DETAILED RESEARCH REPORT (USE SPECIFIC FACTS FROM THIS):
{markdown_report if markdown_report else 'No detailed report available - use supplementary sources below.'}

==== SUPPLEMENTARY CONTEXT ====
Company Summary: {airtable_context.get('summary', 'N/A')}
Additional Notes: {airtable_context.get('note', 'N/A')}

Additional Research Files from Drive:
{research_summary}

==== INSTRUCTIONS ====
1. Use the Strategic Angle to guide your approach
2. Pull 1-2 specific facts from the Markdown Report to demonstrate research
3. Keep the length and tone similar to the template
4. Be direct and professional - avoid flowery language
5. Return as JSON with "subject" and "body" fields"""

            # Call the OpenAI API
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.3,
                    max_tokens=2000,
                    response_format={"type": "json_object"},
                )

                generated_text = response.choices[0].message.content.strip()
                if not generated_text:
                    raise EmailGenerationError("OpenAI API returned empty response")

                # Parse the JSON response
                try:
                    result = json.loads(generated_text)
                    email_body = result.get("body", "").strip()
                    subject_line = result.get("subject", "").strip()

                    if not email_body:
                        raise EmailGenerationError("Generated email body is empty")
                    if not subject_line:
                        # Fallback to company name if no subject provided
                        subject_line = f"Following up - {airtable_context.get('name', 'Quick question')}"

                    return email_body, subject_line

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    logger.error(f"Raw response: {generated_text}")
                    raise EmailGenerationError(
                        f"Failed to parse AI response as JSON: {str(e)}"
                    )

            except Exception as api_error:
                raise EmailGenerationError(f"OpenAI API error: {str(api_error)}")

        except Exception as e:
            if isinstance(e, EmailGenerationError):
                raise
            raise EmailGenerationError(f"Error generating email: {str(e)}")
