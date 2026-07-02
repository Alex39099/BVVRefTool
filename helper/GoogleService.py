import logging
import os
from pathlib import Path

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.credentials import Credentials as BaseCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from AppConfig import AuthorizationType, GoogleSettings

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the token file for oauth
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"] # ["https://www.googleapis.com/auth/spreadsheets"]

def authorize(settings: GoogleSettings) -> BaseCredentials:
    """Authorize to Google Cloud using either service account or OAuth credentials based on the provided settings.

    Args:
        settings (GoogleSettings): config settings to use for authorization

    Raises:
        ValueError: if settings are missing or invalid

    Returns:
        BaseCredentials: credentials to use with Google APIs
    """
    if settings.authorization_type == AuthorizationType.SERVICE_ACCOUNT:
        if not settings.service_account_authorization:
            raise ValueError("service_account_authorization settings are required for service account authorization")
        return _authorize_service_account(
            settings.service_account_authorization.service_account_key_file_path,
            settings.service_account_authorization.impersonate_user
        )
    elif settings.authorization_type == AuthorizationType.OAUTH:
        if not settings.oauth_authorization:
            raise ValueError("oauth_authorization settings are required for oauth authorization")
        return _authorize_oauth(
            settings.oauth_authorization.oauth_client_file_path,
            settings.oauth_authorization.oauth_token_file_path
        )
    else:
        raise ValueError(f"Unknown authorization type: {settings.authorization_type}")

def _authorize_oauth(oauth_file_path: Path | str | None, token_file_path: Path | str = "gc_token.json") -> BaseCredentials:
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

def _authorize_service_account(service_account_file: Path | str, impersonate_user: str | None) -> BaseCredentials:
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    if impersonate_user:
        creds = creds.with_subject(impersonate_user)
    return creds

def read_spreadsheet_data(spreadsheet_id: str, range_name: str, credentials: BaseCredentials) -> list[list[str | int | float | bool]]:
    """Read spreadsheet data by id and range.

    Args:
        spreadsheet_id (str): id of the spreadsheet to read
        range_name (str): range within the spreadsheet to read
        credentials (BaseCredentials): credentials to use for authorization

    Raises:
        HttpError: if the request to the Google Sheets API fails

    Returns:
        list[list[str | int | float | bool]]: spreadsheet data of the specified range
    """
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
        raise e
