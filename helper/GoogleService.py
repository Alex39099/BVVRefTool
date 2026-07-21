import logging
import os
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.credentials import Credentials as BaseCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.AppConfig import AuthorizationType, GoogleSettings

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the token file for oauth
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/forms.body",
]

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

def get_range_name(sheet_name: str | None, start: str, end: str) -> str:
    """Get the range name for a sheet_name, as well as start and end, such as "A1"

    Args:
        sheet_name (str | None): name of the sheet or None to target the first sheet
        start (str): start cell of the range (e.g., "A1")
        end (str): end cell of the range (e.g., "B2" or "B")

    Returns:
        str: range_name ready to be used in the Google Sheets API, e.g., "'Sheet1'!A1:B2" or "A1:B2"
    """
    return f"'{sheet_name}'!{start}:{end}" if sheet_name else f"{start}:{end}"

def read_spreadsheet_data(spreadsheet_id: str, range_name: str, credentials: BaseCredentials) -> list[list[str | int | float | bool]]:
    """Read spreadsheet data by id and range.

    Args:
        spreadsheet_id (str): id of the spreadsheet to read
        range_name (str): range within the spreadsheet to read
        credentials (BaseCredentials): credentials to use for authorization

    Raises:
        HttpError: if the request to the Google Sheets API fails
        ValueError: if spreadsheet_id or range_name are empty
        Exception: re-raises for any other unexpected errors

    Returns:
        list[list[str | int | float | bool]]: spreadsheet data of the specified range
    """
    
    if not spreadsheet_id:
        raise ValueError("spreadsheet_id must be a non-empty string.")
    if not range_name:
        raise ValueError("range_name must be a non-empty string.")
    
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
        logger.error(f"Sheets API HTTP error while reading from spreadsheet '{spreadsheet_id}', range '{range_name}': {e}")
        logger.exception(e)
        raise
    except Exception as e:
        logger.error(f"Unexpected error while reading from spreadsheet '{spreadsheet_id}', range '{range_name}': {e}")
        raise
    
def write_spreadsheet_data(spreadsheet_id: str, range_name: str, data: list[list[Any]], credentials: BaseCredentials) -> dict:
    """Write data to a Google Sheets spreadsheet using the Sheets API v4.

    Args:
        spreadsheet_id (str): id of the spreadsheet to read
        range_name (str): range within the spreadsheet to read
        data (list[list[Any]]): data to write to the spreadsheet
        credentials (BaseCredentials): credentials to use for authorization

    Raises:
        HttpError: if the request to the Google Sheets API fails
        ValueError: if spreadsheet_id or range_name are empty
        Exception: re-raises for any other unexpected errors

    Returns:
        dict: response from the Google Sheets API after writing the data
    """
    if not spreadsheet_id:
        raise ValueError("spreadsheet_id must be a non-empty string.")
    if not range_name:
        raise ValueError("range_name must be a non-empty string.")
    if data is None:
        raise ValueError("data must not be None.")
    
    try:
        service = build("sheets", "v4", credentials=credentials)

        body: dict[str, Any] = {
            "range": range_name,
            "majorDimension": "ROWS",
            "values": data,
        }

        response: dict = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )

    except HttpError as e:
        logger.error(f"Sheets API HTTP error while writing to spreadsheet '{spreadsheet_id}', range '{range_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while writing to spreadsheet '{spreadsheet_id}', range '{range_name}': {e}")
        raise
    
    logger.info(
        f"Successfully wrote to spreadsheet '{spreadsheet_id}', range '{range_name}'. "
        f"Updated range: '{response.get('updatedRange')}', cells updated: {response.get('updatedCells')}."
    )

    return response

def update_form_dropdown(form_id: str, items: dict[str, list[str]], credentials: BaseCredentials) -> dict:
    """Updates the options of one or more dropdown questions in a Google Form.

    Args:
        form_id (str): The unique identifier of the Google Form to update.
        items (dict[str, list[str]]): A dictionary mapping each item_id (str) to a list of strings
                                    representing the new dropdown options to set on that question.
                                    If any item's option list is empty, a warning is logged and the
                                    update for that item is still attempted.
        credentials (BaseCredentials): credentials to use for authorization

    Returns:
        dict: A dictionary containing the API response from the batchUpdate call, as returned by the Google Forms API v1.
    """
    for item_id, options in items.items():
        if not options:
            logger.warning(
                f"No options provided for form_id {form_id} and item_id {item_id}"
            )
        
    try:
        service = build("forms", "v1", credentials=credentials)

        # Build one updateItem request per item_id in the mapping
        requests = [
            {
                "updateItem": {
                    "item": {
                        "itemId": item_id,
                        "questionItem": {
                            "question": {
                                "choiceQuestion": {
                                    "type": "DROP_DOWN",
                                    # Build the list of choice option dicts expected by the Forms API
                                    "options": [{"value": option} for option in options],
                                }
                            }
                        },
                    },
                    # Specify which fields within the item should be updated
                    "updateMask": "questionItem.question.choiceQuestion.options",
                    "location": {"index": 0},
                }
            }
            for item_id, options in items.items()
        ]

        body = {"requests": requests}

        response = (
            service.forms()
            .batchUpdate(formId=form_id, body=body)
            .execute()
        )

        return response

    except HttpError as e:
        logger.error(f"Could not update dropdown options for form_id {form_id} and item_ids {list(items.keys())}")
        logger.exception(e)        
        raise