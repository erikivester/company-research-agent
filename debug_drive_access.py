import os
import json
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

def create_templates_folder():
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
            scopes=[
                'https://www.googleapis.com/auth/drive.file',
                'https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        
        # Create Drive API client
        service = build('drive', 'v3', credentials=credentials)
        
        # Create a new folder for templates inside the workspace
        drive_id = "0AH84XqkvrUuAUk9PVA"  # Shared drive ID
        
        # Create the file metadata with parent folder in the shared drive
        file_metadata = {
            'name': 'Email Templates',
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': ['0AH84XqkvrUuAUk9PVA']  # Set the shared drive as parent
        }
        
        try:
            # First, get the shared drive info
            drive_info = service.drives().get(
                driveId=drive_id
            ).execute()
            
            print(f"\nFound shared drive: {drive_info.get('name', 'Unknown')}")
            
            # Create the folder in the shared drive
            create_args = {
                'body': file_metadata,
                'fields': 'id, name, webViewLink',
                'supportsTeamDrives': True,  # For backward compatibility
                'supportsAllDrives': True,
                'enforceSingleParent': True
            }
            
            folder = service.files().create(**create_args).execute()
            print(f"Created folder: {folder.get('name', 'Unknown')}")
            
            folder_id = folder.get('id')
            folder_link = folder.get('webViewLink')
            
            print(f"\n✓ Created new templates folder:")
            print(f"Folder Name: {folder['name']}")
            print(f"Folder ID: {folder_id}")
            print(f"Folder Link: {folder_link}")
            
            # Verify it's visible in the drive
            list_result = service.files().list(
                q=f"mimeType='application/vnd.google-apps.folder' and name='{folder['name']}'",
                spaces='drive',
                fields='files(id, name)',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora='allDrives'
            ).execute()
            
            files = list_result.get('files', [])
            if any(f['id'] == folder_id for f in files):
                print("✓ Successfully verified folder is visible in the shared drive")
            else:
                print("⚠️ Warning: Folder was created but not showing in the shared drive. Please check permissions.")
                
            return folder_id, folder_link
            
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            return None, None
        
        folder_id = folder['id']
        folder_link = folder['webViewLink']
        
        print(f"\n✓ Created new templates folder:")
        print(f"Folder Name: {folder['name']}")
        print(f"Folder ID: {folder['id']}")
        print(f"Folder Link: {folder.get('webViewLink', 'Not available')}")
        
        # Verify it's visible in the drive
        list_result = service.files().list(
            q=f"mimeType='application/vnd.google-apps.folder' and name='{folder['name']}'",
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora='allDrives'
        ).execute()
        
        files = list_result.get('files', [])
        if any(f['id'] == folder_id for f in files):
            print("✓ Successfully verified folder is visible in the shared drive")
        else:
            print("⚠️ Warning: Folder was created but not showing in the shared drive. Please check permissions.")
        
        # Update the folder ID in email_templates.py
        templates_file = Path(__file__).parent / 'backend' / 'utils' / 'email_templates.py'
        if templates_file.exists():
            with open(templates_file, 'r') as f:
                content = f.read()
            
            # Replace the folder ID
            updated_content = content.replace(
                '"1tt4LLouNP2FgHcguIKlnRzRb3j5jE8LH"',
                f'"{folder_id}"'
            )
            
            with open(templates_file, 'w') as f:
                f.write(updated_content)
            
            print("\n✓ Updated folder ID in email_templates.py")
        
        return folder_id, folder_link
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return None, None
    try:
        # Get credentials from environment variable
        creds_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("GDRIVE_CREDENTIALS_JSON not found in environment")
        
        creds_info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        
        # Create Drive API client
        service = build('drive', 'v3', credentials=credentials)
        
        # List files in the templates folder
        folder_id = "1tt4LLouNP2FgHcguIKlnRzRb3j5jE8LH"  # Your template folder ID
        
        print(f"\nChecking folder: {folder_id}")
        
        # First, verify the folder exists and get its details
        try:
            folder = service.files().get(fileId=folder_id).execute()
            print(f"✓ Found folder: {folder['name']} (type: {folder['mimeType']})")
        except Exception as e:
            print(f"❌ Error accessing folder: {str(e)}")
            return
        
        # Then list all files in the folder
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType, owners, permissions)',
            pageSize=50
        ).execute()
        
        files = results.get('files', [])
        print(f"\nFound {len(files)} files in folder:")
        for file in files:
            print(f"\n📄 File: {file['name']}")
            print(f"   Type: {file['mimeType']}")
            print(f"   ID: {file['id']}")
            if 'permissions' in file:
                print("   Permissions:")
                for perm in file['permissions']:
                    print(f"   - {perm.get('emailAddress', 'N/A')} ({perm.get('role', 'N/A')})")
        
        # Also check if our service account has access
        sa_email = creds_info.get('client_email')
        print(f"\nService Account Email: {sa_email}")
        
        # Try to list parent folder to check permissions
        parent_results = service.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        print("\n✓ Service account has required permissions (can list folder contents)")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    folder_id, folder_link = create_templates_folder()
    if folder_id:
        print("\nNext steps:")
        print("1. Open the folder link in your browser")
        print("2. Upload your email template files")
        print("3. Make sure to share the folder with your service account email:")
        print(f"   {json.loads(os.getenv('GDRIVE_CREDENTIALS_JSON'))['client_email']}")