from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pickle

SCOPES = ['https://www.googleapis.com/auth/drive.file']  # or 'drive' for full access

def get_user_creds():
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)
    return creds

if __name__ == '__main__':
    creds = get_user_creds()
    service = build('drive', 'v3', credentials=creds)
    # Test: list first 10 files
    results = service.files().list(pageSize=10, fields="files(id, name)").execute()
    for f in results.get('files', []):
        print(f['name'], f['id'])
