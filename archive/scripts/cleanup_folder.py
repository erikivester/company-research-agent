import os
import json
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

def delete_folder(folder_id):
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
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        # Create Drive API client
        service = build('drive', 'v3', credentials=credentials)
        
        # Delete the folder
        service.files().delete(
            fileId=folder_id,
            supportsAllDrives=True
        ).execute()
        
        print(f"✓ Successfully deleted folder {folder_id}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    # ID of the folder to delete
    folder_to_delete = "1C3P0znHFisQ-f9MrAIRYlFH9nJqKqPlD"
    delete_folder(folder_to_delete)