import logging
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the token file
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"] # ["https://www.googleapis.com/auth/spreadsheets"]


def authorize(oauth_file_path: str | None, token_file_path: str = "gc_token.json") -> Credentials:
    if not token_file_path:
        raise ValueError("token_file_path is required")

    creds = None
    if os.path.exists(token_file_path):
        creds = Credentials.from_authorized_user_file(token_file_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.debug("refreshing authentication token...")
            creds.refresh(Request())
        elif oauth_file_path:
            logger.info("no valid Google credentials, user most log in")
            flow = InstalledAppFlow.from_client_secrets_file(oauth_file_path, SCOPES)
            creds = flow.run_local_server(port=0)
        else:
            raise ValueError(f"no token file at {token_file_path} and no credentials file at {oauth_file_path}")
        # Save the credentials for the next run
        with open(token_file_path, "w") as token:
            token.write(creds.to_json())

    return creds


def read_spreadsheet_data(spreadsheet_id: str, range_name: str, credentials: Credentials):
    try:
        service = build("sheets", "v4", credentials=credentials)
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        values = result.get("values", [])
        if not values:
            logger.warning(f"No data found for spreadsheet_id {spreadsheet_id} and range {range_name}")
        return values
    except HttpError as e:
        logger.error(f"Could not load spreadsheet data for spreadsheet_id {spreadsheet_id} and range {range_name}")
        logger.exception(e)
