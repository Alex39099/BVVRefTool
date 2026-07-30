import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from googleapiclient.discovery import build

from manager.models import Course

logger = logging.getLogger(__name__)
    

class BaseSheet(ABC):
    
    def __init__(self) -> None:
        super().__init__()
        self._jsonsheet = 
    


class CourseSheetStatus(StrEnum):
    OPEN = "Offen" # initial state
    IN_REVIEW = "In Prüfung"
    READY_FOR_SUBMISSION = "Bereit zur Übermittlung (BVV)"
    COMPLETED = "Abgeschlossen" # terminal state
    
class CourseSheetRegistrationStatus(StrEnum):
    OPEN = "Offen" # initial state
    IN_REVIEW = "In Prüfung"
    CANCELLED = "Storniert" # terminal state
    DENIED = "Abgelehnt" # terminal state
    READY_FOR_SUBMISSION = "Wartet auf Übermittlung (BVV)"
    REGISTERED = "Angemeldet (BVV)" # terminal state
    
@dataclass
class CourseSheetRegistration:
    team_name: str
    team_priority: int
    member_token: str
    status: CourseSheetRegistrationStatus
    priority: int | None
    
    @classmethod
    def from_row(cls, row: list[str]):
        return cls(
            team_name=row[0],
            team_priority=int(row[1].removeprefix("Prio").strip()),
            member_token=row[2],
            status=CourseSheetRegistrationStatus(row[3]),
            priority=int(row[4]) if row[4] else None
        )

def get_range_name(sheet_name: str, bounds: tuple[str, str]):
    return f"{sheet_name}!{bounds[0]}:{bounds[1]}"

def get_grid_range(sheet_id: int, bounds: tuple[str, str] | None):
    if bounds is None:
        return {"sheetId": sheet_id}  # whole sheet
    
    pattern = re.compile(r"^([A-Za-z]+)(\d+)$")
    
    start_match = pattern.fullmatch(bounds[0])
    end_match = pattern.fullmatch(bounds[1])

    if not start_match or not end_match:
        raise ValueError(f"Invalid bounds: {bounds}")
    
    start_col, start_row = start_match.groups()
    end_col, end_row = end_match.groups()
    
    def _column_to_index(column: str) -> int:
        index = 0
        for c in column.upper():
            index = index * 26 + (ord(c) - ord("A") + 1)
        return index - 1
    
    return {
        "sheetId": sheet_id,
        "startRowIndex": int(start_row) - 1,
        "endRowIndex": int(end_row), # exclusive
        "startColumnIndex": _column_to_index(start_col),
        "endColumnIndex": _column_to_index(end_col) + 1 # exclusive
    }

class CourseSheet(BaseSheet):
    
    _COURSE_INFO_RANGE = ("B2", "C9")
    _HOW_TO_RANGE = ("B11", "C11")
    
    _REGISTRATION_TABLE_START = "B15"
    _REGISTRATION_TABLE_COLOR_GREY_RGB = (0.9372549, 0.9372549, 0.9372549)
    _REGISTRATION_COUNT_PER_TEAM = 5
    
    _id: str
    _title: str
    _status: CourseSheetStatus
    team_names: list[str]
    registrations: list[CourseSheetRegistration] # pos of registrations?
    
    # TODO after creation, everything should be frozen except title, status and registrations
    # TODO registrations should only be changeable through functions of this class to flag changes, i.e. make CourseSheetRegistration frozen
    
    # TODO when creating the sheet, the id should mimic the course id if possible
    # TODO when creating protectedRanges, use bitPacking
    #       protected_range_id = (course_id << 4) | range_type
    #       course_id = protected_range_id >> 4
    #       range_type = protected_range_id & 0xF
    # class RangeType:
    # SHEET = 0
    # HEADER = 1
    # FORMULAS = 2
    # RESULTS = 3
    
    @property
    def id(self) -> str:
        return self._id
    
    @property
    def title(self) -> str:
        return self._title
    
    @property
    def status(self) -> CourseSheetStatus:
        return self._status
    
    @classmethod
    def create(cls, spreadsheet_id: str, template_sheet_id: str, course: Course, team_names: list[str]) -> CourseSheet:
        # copy template wks
        # insert header table values
        # create space for amount of teams
        # fill in team names
        # update drop down Mitglied to corresponding Members column
        # create protected ranges. We need to protect A:C, E:I, D1:D14. 
        # update OverviewSheet?
        pass
    
    def update_course_data(self, course: Course):
        # update header table values
        pass
    
    def start_review(self):
        # Open -> In Review
        pass
    
    def prepare_submission(self):
        # In Review -> Ready for submission
        # update every registration status, open -> (cancelled or in_review)
        pass
    
    def mark_as_submitted(self):
        pass
    
    def protect(self):
        # protect whole sheet
        pass

class OverviewSheet(BaseSheet):
    pass

class MemberSheet(BaseSheet):
    pass
    
    
class RegistrationSpreadsheet:
    
    course_sheets: list[CourseSheet]
    course_template_sheet: Any
    member_sheet: MemberSheet
    overview_sheet: OverviewSheet
    
    def __init__(self, credentials) -> None:
        # Google Sheets API: https://developers.google.com/workspace/sheets/api/guides/concepts
        self._gservice: Any = build("sheets", "v4", credentials).spreadsheets()
        
        self.load()
    
    def load(self):
        # load data from cloud
        pass
    
    def commit(self):
        # commit data to cloud
        pass
    
    def create_course_sheet(self):
        pass
    