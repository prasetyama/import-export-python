import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def upload_file_to_gdrive(filepath, folder_id, credentials_json):
    """
    Uploads a file to Google Drive in the specified folder.
    Args:
        filepath (str): Path to the file to upload.
        folder_id (str): Google Drive folder ID.
        credentials_json (str): Path to service account credentials JSON.
    Returns:
        file_id (str): The ID of the uploaded file, or None if failed.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_json,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        service = build('drive', 'v3', credentials=credentials)
        file_metadata = {
            'name': os.path.basename(filepath),
            'parents': [folder_id]
        }
        media = MediaFileUpload(filepath, resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        ).execute()
        return file.get('id')
    except Exception as e:
        print(f"Failed to upload to GDrive: {e}")
        return None
