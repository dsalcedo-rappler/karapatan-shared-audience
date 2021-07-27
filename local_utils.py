def authorize_api(token_file = "token.json"):
    """
    Authorizes you to use Google APIs directly
    Upload the token.json file to this google colab first
    """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    
    SCOPES = [
        'https://www.googleapis.com/auth/calendar',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/documents',
        'https://www.googleapis.com/auth/youtube'
    ]
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    drive = build('drive', 'v3', credentials=creds)
    sheets = build('sheets', 'v4', credentials=creds)
    docs = build('docs','v1', credentials=creds)
    calendar = build('calendar', 'v3', credentials=creds)
    youtube = build('youtube', 'v3', credentials=creds)

    return {'drive': drive, 'sheets': sheets, 'docs': docs, 'calendar': calendar, 'youtube': youtube}

def download_from_gdrive(link, colab_filename = 'Filename.csv'):
    """
    Downloads a csv form google drive and reutrns it as a pandas dataframe
    """
    import io
    import pandas as pd
    from googleapiclient.http import MediaIoBaseDownload

    drive = authorize_api()['drive']
    gdrive_id = link.split('/')[5]

    request = drive.files().get_media(fileId = gdrive_id) 
    fh = io.FileIO(colab_filename,'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    return pd.read_csv(colab_filename, dtype=str)