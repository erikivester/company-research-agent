# backend/utils/gdrive_uploader.py
import os
import io
import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, 'gdrive_credentials.json')
# --- END MODIFICATION ---

# Define the scopes required for Google Drive API
# Use the full Drive scope so the service account (or delegated user)
# can list Shared Drives and manage files across drives.
SCOPES = ['https://www.googleapis.com/auth/drive']

# --- HELPER: Get Google Drive Service ---
def get_drive_service():
    """Authenticates and returns a Google Drive API service object."""
    
    creds = None
    if os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON"):
        try:
            creds_json = json.loads(os.environ["GDRIVE_SERVICE_ACCOUNT_JSON"])
            creds = service_account.Credentials.from_service_account_info(creds_json, scopes=SCOPES)
            logger.info("Loaded Google Drive credentials from ENV variable.")
        except Exception as e:
            logger.warning(f"Failed to load GDrive credentials from ENV: {e}")
    
    if not creds:
        try:
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES
            )
            logger.info(f"Loaded Google Drive credentials from file: {SERVICE_ACCOUNT_FILE}")
        except FileNotFoundError:
            logger.error(f"CRITICAL: Google Drive credentials file not found at {SERVICE_ACCOUNT_FILE} and env var not set.")
            return None
        except Exception as e:
            logger.error(f"Error building Google Drive service from file: {e}", exc_info=True)
            return None

    delegate_user = os.getenv("GDRIVE_DELEGATE_USER")
    if delegate_user:
        try:
            creds = creds.with_subject(delegate_user)
            logger.info(f"Using delegated credentials to impersonate: {delegate_user}")
        except Exception as e:
            logger.warning(f"Failed to delegate credentials to {delegate_user}: {e}", exc_info=False)

    service = None 
    try:
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        sa_email = getattr(creds, 'service_account_email', None)
        if sa_email:
            logger.info(f"Google Drive credentials loaded for service account: {sa_email}")
            
            try:
                drives = service.drives().list(fields="drives(id,name)").execute()
                drive_count = len(drives.get('drives', []))
                if drive_count == 0:
                    logger.warning("⚠️ SA cannot see any Shared Drives! Add SA as member to the target Shared Drive.")
                else:
                    logger.info(f"✓ SA can see {drive_count} Shared Drive(s)")
            except Exception as e:
                logger.warning(f"⚠️ SA may not have permissions to list Shared Drives (this is ok if it's added as a member): {e}")
    except Exception as e:
        logger.warning(f"Could not run SA diagnostics: {e}")
        
    if not service:
        try:
            service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        except Exception as e:
            logger.error(f"Error building Google Drive service: {e}", exc_info=True)
            return None

    return service


# --- HELPER: Extract Folder ID from URL ---
def _extract_folder_id_from_url(folder_url: str) -> Optional[str]:
    """Extracts the Google Drive Folder ID from a standard URL."""
    if not folder_url or 'drive.google.com' not in folder_url:
        return None
    
    for prefix in ['/folders/', '/drive/folders/']:
        parts = folder_url.split(prefix)
        if len(parts) > 1:
            folder_id = parts[-1].split('?')[0]
            logger.debug(f"Extracted folder ID from URL: {folder_id}")
            return folder_id

    logger.warning(f"Could not parse Folder ID from URL: {folder_url}")
    return None

# --- CORE ASYNC UPLOAD FUNCTION ---
async def upload_context_to_gdrive(
    context: Dict[str, Any] | bytes | io.BytesIO, 
    folder_url: str, 
    file_name: str,
    content_type: str = 'application/json'
):
    """
    Authenticates with Google Drive and uploads a file to the specified folder
    (which must be in a Shared Drive).
    """
    
    folder_id = _extract_folder_id_from_url(folder_url)
    if not folder_id:
        raise ValueError(f"Invalid Google Drive folder URL provided: {folder_url}")

    service = await asyncio.to_thread(get_drive_service)
    if not service:
        raise ConnectionError("Failed to authenticate Google Drive service. Check credentials.")

    logger.info(f"Uploading '{file_name}' to GDrive Folder ID: {folder_id}...")

    try:
        if content_type == 'application/json':
            content = json.dumps(context, indent=2).encode('utf-8')
        elif content_type == 'application/pdf':
            if isinstance(context, io.BytesIO):
                content = context.getvalue()
            else:
                content = context.encode('utf-8') if isinstance(context, str) else context
        else:
            content = context.encode('utf-8') if isinstance(context, str) else str(context).encode('utf-8')
        
        media_buffer = io.BytesIO(content)
    except Exception as e:
        logger.error(f"Failed to process content for upload: {e}")
        raise

    file_metadata = {
        'name': file_name,
        'parents': [folder_id],
        'mimeType': content_type
    }
    
    media_buffer.seek(0)
    media = MediaIoBaseUpload(
        media_buffer,
        mimetype=content_type,
        resumable=True
    )

    try:
        def _execute_upload():
            query = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
            
            existing_files = service.files().list(
                q=query, 
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True # This IS valid for list()
            ).execute()
            
            existing_file = existing_files.get('files', [])
            
            if existing_file:
                file_id = existing_file[0].get('id')
                logger.debug(f"File '{file_name}' already exists. Updating existing file ID: {file_id}")
                request = service.files().update(
                    fileId=file_id,
                    media_body=media,
                    fields='id',
                    supportsAllDrives=True
                )
            else:
                logger.debug(f"File '{file_name}' not found. Creating new file.")
                request = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id',
                    supportsAllDrives=True
                )
            
            file = request.execute()
            return file.get('id')
        
        file_id = await asyncio.to_thread(_execute_upload)
        logger.info(f"Successfully uploaded/updated file. File ID: {file_id}")

    except Exception as e:
        logger.error(f"Failed to upload file '{file_name}' to Google Drive: {e}", exc_info=True)
        raise
    finally:
        media_buffer.close()


async def upload_research_with_pdf(
    research_data: Dict[str, Any],
    folder_url: str,
    company_name: str
) -> Dict[str, str]:
    """
    Uploads both JSON research data and a PDF version to Google Drive.
    
    Args:
        research_data: The complete research data dictionary
        folder_url: Google Drive folder URL
        company_name: Name of the company for file naming
    
    Returns:
        Dict with file IDs for both JSON and PDF uploads
    """
    from backend.services.pdf_service import PDFService
    
    results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Upload JSON data
    json_filename = f"{company_name.lower().replace(' ', '_')}_research_{timestamp}.json"
    try:
        await upload_context_to_gdrive(
            research_data,
            folder_url,
            json_filename,
            'application/json'
        )
        results['json_file'] = json_filename
    except Exception as e:
        logger.error(f"Failed to upload JSON research data: {e}")
        raise

    # 2. Generate and upload PDF
    try:
        pdf_service = PDFService({"pdf_output_dir": "pdfs"})
        success, pdf_result = await pdf_service.generate_pdf_stream(research_data, company_name)
        
        if success:
            pdf_buffer, pdf_filename = pdf_result
            await upload_context_to_gdrive(
                pdf_buffer,
                folder_url,
                pdf_filename,
                'application/pdf'
            )
            results['pdf_file'] = pdf_filename
        else:
            logger.error(f"Failed to generate PDF: {pdf_result}")
            
    except Exception as e:
        logger.error(f"Failed to upload PDF version: {e}")
        # Don't raise here - we still uploaded the JSON successfully

    return results

def inspect_drive_folder(folder_url: str) -> Dict[str, Any]:
    """Return metadata for a Drive folder (works with Shared Drives)."""
    folder_id = _extract_folder_id_from_url(folder_url)
    if not folder_id:
        raise ValueError(f"Invalid Google Drive folder URL provided: {folder_url}")

    service = get_drive_service()
    if not service:
        raise ConnectionError("Failed to authenticate Google Drive service. Check credentials.")

    try:
        # --- FIX: Removed 'includeItemsFromAllDrives' from get() call ---
        resp = service.files().get(
            fileId=folder_id,
            supportsAllDrives=True,
            fields="id,name,mimeType,driveId,owners,permissions,capabilities"
        ).execute()
        # --- END FIX ---

        owners = [o.get('emailAddress') for o in resp.get('owners', []) if o.get('emailAddress')]
        permissions = []
        for p in resp.get('permissions', []):
            permissions.append({
                'id': p.get('id'),
                'type': p.get('type'),
                'role': p.get('role'),
                'emailAddress': p.get('emailAddress')
            })

        drive_id = resp.get('driveId')
        if drive_id:
            logger.info(f"Target folder is in Shared Drive with ID: {drive_id}")

        summary = {
            'id': resp.get('id'),
            'name': resp.get('name'),
            'mimeType': resp.get('mimeType'),
            'driveId': resp.get('driveId'),
            'owners': owners,
            'permissions': permissions,
            'capabilities': resp.get('capabilities', {}),
            'raw': resp
        }
        return summary

    except Exception as e:
        logger.error(f"Failed to inspect Drive folder {folder_url}: {e}", exc_info=True)
        raise