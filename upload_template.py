import os
import json
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def upload_template():
    # Load environment variables from .env file
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        
    try:
        # Get credentials from environment variable
        creds_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("GDRIVE_CREDENTIALS_JSON not found in environment")
        
        creds_info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        
        # Create Drive API client
        service = build('drive', 'v3', credentials=credentials)
        
        # Template folder ID
        folder_id = "1h_U3DyDXP1VX6E999zRlti_-xLeRkWOW"
        
        # Path to local template file
        template_path = "templates/STANDARD_INTRO.md"
        
        # Prepare the file metadata
        file_metadata = {
            'name': 'STANDARD_INTRO.md',
            'parents': [folder_id],
            'mimeType': 'text/markdown'
        }
        
        # Create media
        media = MediaFileUpload(
            template_path,
            mimetype='text/markdown',
            resumable=True
        )
        
        # Upload the file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webViewLink',
            supportsAllDrives=True
        ).execute()
        
        print(f"✓ Template uploaded successfully!")
        print(f"File ID: {file.get('id')}")
        print(f"File name: {file.get('name')}")
        print(f"View link: {file.get('webViewLink')}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    upload_template()