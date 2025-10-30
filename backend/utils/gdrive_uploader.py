# backend/utils/gdrive_uploader.py
import os
import io
import json
import logging
import asyncio
from typing import Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
# Define the path to your service account credentials file.
# This assumes 'gdrive_credentials.json' is in the project root directory.
SERVICE_ACCOUNT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
    'gdrive_credentials.json'
)
# Define the scopes required for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# --- HELPER: Get Google Drive Service ---
def get_drive_service():
    """Authenticates and returns a Google Drive API service object."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        return service
    except FileNotFoundError:
        logger.error(f"CRITICAL: Google Drive credentials file not found at {SERVICE_ACCOUNT_FILE}.")
        logger.error("Please create a service account and place 'gdrive_credentials.json' in the project root.")
        return None
    except Exception as e:
        logger.error(f"Error building Google Drive service: {e}", exc_info=True)
        return None

# --- HELPER: Extract Folder ID from URL ---
def _extract_folder_id_from_url(folder_url: str) -> Optional[str]:
    """Extracts the Google Drive Folder ID from a standard URL."""
    if not folder_url or 'drive.google.com' not in folder_url:
        return None
    
    # Standard URL format: .../folders/FOLDER_ID
    parts = folder_url.split('/folders/')
    if len(parts) > 1:
        return parts[-1].split('?')[0] # Clean query params
        
    # Other possible format: .../drive/u/0/folders/FOLDER_ID
    parts = folder_url.split('/folders/')
    if len(parts) > 1:
        return parts[-1].split('?')[0]

    logger.warning(f"Could not parse Folder ID from URL: {folder_url}")
    return None

# --- CORE ASYNC UPLOAD FUNCTION ---
async def upload_context_to_gdrive(
    context: Dict[str, Any], 
    folder_url: str, 
    file_name: str
):
    """
    Authenticates with Google Drive and uploads a JSON file of the
    research context to the specified folder.
    
    This function is designed to be called with asyncio.to_thread
    as the Google API client library is synchronous.
    """
    
    folder_id = _extract_folder_id_from_url(folder_url)
    if not folder_id:
        raise ValueError(f"Invalid Google Drive folder URL provided: {folder_url}")

    service = await asyncio.to_thread(get_drive_service)
    if not service:
        raise ConnectionError("Failed to authenticate Google Drive service. Check credentials.")

    logger.info(f"Uploading '{file_name}' to GDrive Folder ID: {folder_id}...")

    # Convert the context dictionary to JSON bytes
    try:
        json_content = json.dumps(context, indent=2)
        media_buffer = io.BytesIO(json_content.encode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to serialize context to JSON: {e}")
        raise

    # Define the file metadata
    file_metadata = {
        'name': file_name,
        'parents': [folder_id],
        'mimeType': 'application/json'
    }
    
    # Create the media upload object
    media = MediaIoBaseUpload(
        media_buffer,
        mimetype='application/json',
        resumable=True
    )

    try:
        # --- Run the synchronous upload in a separate thread ---
        def _execute_upload():
            # Check if file with the same name already exists in this folder
            query = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
            existing_files = service.files().list(q=query, fields="files(id)").execute()
            
            existing_file = existing_files.get('files', [])
            
            if existing_file:
                # UPDATE existing file
                file_id = existing_file[0].get('id')
                logger.debug(f"File '{file_name}' already exists. Updating existing file ID: {file_id}")
                request = service.files().update(
                    fileId=file_id,
                    media_body=media,
                    fields='id'
                )
            else:
                # CREATE new file
                logger.debug(f"File '{file_name}' not found. Creating new file.")
                request = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                )
            
            file = request.execute()
            return file.get('id')
        
        # Run the blocking I/O operation in a thread
        file_id = await asyncio.to_thread(_execute_upload)
        
        logger.info(f"Successfully uploaded/updated file. File ID: {file_id}")

    except Exception as e:
        logger.error(f"Failed to upload file '{file_name}' to Google Drive: {e}", exc_info=True)
        raise
    finally:
        media_buffer.close()