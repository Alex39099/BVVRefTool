from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import ClassVar

from helper.google_api.sheets.data_validation import (
    DataValidation,
    DropDownRangeValidationRule,
)
from helper.google_api.sheets.grid_range import GridRange
from helper.google_api.sheets.protected_range import ProtectedRange
from helper.google_api.sheets.sheet import Sheet
from helper.google_api.sheets.spreadsheet import Spreadsheet
from manager.models import (
    Course,
    ParticipationStatus,
    PersonIdentity,
    RegistrationStatus,
)
from registration_srv.util import get_person_from_token, get_token_from_person

logger = logging.getLogger(__name__)


class CourseSheetStatus(StrEnum):
    OPEN = "Offen für Anmeldungen" # initial state
    IN_REVIEW = "In Prüfung"
    READY_FOR_SUBMISSION = "Bereit zur Übermittlung (BVV)"
    CLOSED = "Geschlossen"  # only for Fortbildung Online
    COMPLETED = "Abgeschlossen" # terminal state
    
class CourseSheetRegistrationStatus(StrEnum):
    OPEN = "Offen" # initial state
    IN_REVIEW = "In Prüfung"
    CANCELLED = "Storniert" # terminal state
    DENIED = "Abgelehnt" # terminal state
    READY_FOR_SUBMISSION = "Akzeptiert (wartet auf Übermittlung (BVV))"
    REGISTERED = "Angemeldet (BVV)" # terminal state
    DUPLICATE = "Fehler (Duplikat)"  # terminal state
    ERROR = "Storniert (Fehler)"  # terminal state
    
class CourseSheetBVVStatus(StrEnum):
    PASSED = "Erfolgreich teilgenommen"
    FAILED = "Nicht erfolgreich teilgenommen"
    MISSED = "Nicht teilgenommen"
    APPROVED = "Zugelassen"
    CANCELLED = "Storniert"
    WAITING = "Auf Warteliste"
    ERROR = "Storniert (Fehler)"
    
    @classmethod
    def get_status(cls, registration_status: RegistrationStatus, participation_status: ParticipationStatus) -> CourseSheetBVVStatus:
        if registration_status == RegistrationStatus.CANCELLED:
            return cls.CANCELLED
        if registration_status == RegistrationStatus.WAITING:
            return cls.WAITING
        # assert registration_status == APPROVED
        if participation_status == ParticipationStatus.PASSED:
            return cls.PASSED
        if participation_status == ParticipationStatus.FAILED:
            return cls.FAILED
        if participation_status == ParticipationStatus.MISSED:
            return cls.MISSED
        if participation_status == ParticipationStatus.PENDING:
            return cls.APPROVED
        raise NotImplementedError(f"not implemented for {registration_status!r} and {participation_status!r}")
    
class CourseSheetRegistration(ABC):
    _sheet: Sheet
    _row_idx: int # absolute row index in the sheet

    def __init__(self, sheet: Sheet, row_idx: int):
        self._sheet = sheet
        self._row_idx = row_idx
        
    @classmethod
    @abstractmethod
    def field_col_idx_mapping(cls) -> dict[str, int]:
        """ Column indices for the registration columns. Must contain keys team_name, member, status and status_bvv """
        ...
        
    @classmethod
    def get_field_col_idx(cls, field_name: str) -> int:
        return cls.field_col_idx_mapping()[field_name]
        
    @property
    @abstractmethod
    def team_name(self) -> str | None:
        ...

    @property
    def member(self) -> PersonIdentity | None:
        token = self._sheet[(self._row_idx, self.get_field_col_idx('member'))]
        if not token:
            return None
        return get_person_from_token(str(token))
    
    @member.setter
    def member(self, value: PersonIdentity | None):
        if value is None:
            self._sheet[(self._row_idx, self.get_field_col_idx('member'))] = None
            return
        token = get_token_from_person(value)
        self._sheet[(self._row_idx, self.get_field_col_idx('member'))] = token

    @property
    def status(self) -> CourseSheetRegistrationStatus:
        return CourseSheetRegistrationStatus(self._sheet[(self._row_idx, self.get_field_col_idx('status'))])

    @status.setter
    def status(self, value: CourseSheetRegistrationStatus) -> None:
        if not isinstance(value, CourseSheetRegistrationStatus):
            raise TypeError("status must be of type CourseSheetRegistrationStatus")
        self._sheet[(self._row_idx, self.get_field_col_idx('status'))] = value.value
        # TODO protect registration for certain status
        
    @property
    def status_bvv(self) -> CourseSheetBVVStatus | None:
        raw = self._sheet[(self._row_idx, self.get_field_col_idx('status_bvv'))]
        return CourseSheetBVVStatus(raw) if raw is not None else None
    
    @status_bvv.setter
    def status_bvv(self, value: CourseSheetBVVStatus | None):
        if value is None:
            self._sheet[(self._row_idx, self.get_field_col_idx('status_bvv'))] = None
            return
        if isinstance(value, CourseSheetBVVStatus):
            self._sheet[(self._row_idx, self.get_field_col_idx('status_bvv'))] = value.value
            return
        raise TypeError("status_bvv must be None or of type CourseSheetBVVStatus")
        

class OpenCourseSheetRegistration(CourseSheetRegistration):
    
    def __init__(self, sheet: Sheet, row_idx: int):
        super().__init__(sheet, row_idx)
        
    @classmethod
    def field_col_idx_mapping(cls) -> dict[str, int]:
        return {
            "team_name": 1,
            "member": 2,
            "status": 3,
            "status_bvv": 4,
        }
        
    @property
    def team_name(self) -> str | None:
        raw = self._sheet[(self._row_idx, self.get_field_col_idx('team_name'))]
        return str(raw) if raw else None

class ManagedCourseSheetRegistration(CourseSheetRegistration):
    _team_name: str
        
    def __init__(self, sheet: Sheet, row_idx: int, team_name: str):
        super().__init__(sheet, row_idx)
        self._team_name = team_name
        
    @classmethod
    def field_col_idx_mapping(cls) -> dict[str, int]:
        return {
            "team_name": 1,
            "team_priority": 2,
            "member": 3,
            "status": 4,
            "status_bvv": 5,
            "priority": 6
        }
        
    @property
    def team_name(self) -> str:
        return self._team_name
    
    @property
    def team_priority(self) -> int:
        raw = str(self._sheet[(self._row_idx, self.get_field_col_idx('team_priority'))]).removeprefix('Prio').strip()
        return int(raw)
    
    @property
    def priority(self) -> int | None:
        raw = self._sheet[(self._row_idx, self.get_field_col_idx('priority'))]
        if not raw:
            return None
        return int(str(raw))

    @priority.setter
    def priority(self, value: int | None) -> None:
        if value is not None and not isinstance(value, int):
            raise TypeError("priority must be of type int")
        self._sheet[(self._row_idx, self.get_field_col_idx('priority'))] = value
    
    

class CourseSheet:
    _sheet: Sheet
    _date_format: ClassVar[str] = '%d.%m.%Y'
    
    def __init__(self, sheet: Sheet, course: Course | None = None, applicable_members: DropDownRangeValidationRule | None = None):
        """ Internal. Construct via RegistrationSpreadsheet """
        self._sheet = sheet
        if course is not None:
            if applicable_members is None:
                raise ValueError("applicable_members must be specified")
            self._sheet.developer_metadata['course'] = course.to_json()
            self._construct_from_template(applicable_members=applicable_members)
            
    @property
    @abstractmethod
    def _field_positions(self) -> dict[str, str]:
        ...
    
    @abstractmethod   
    def _construct_from_template(self, applicable_members: DropDownRangeValidationRule) -> None:
        ...
        
    @classmethod
    def from_cloud(cls, sheet: Sheet) -> CourseSheet:
        return cls(sheet)
    
    @property
    def course(self) -> Course:
        return Course.from_json(self._sheet.developer_metadata.data['course']) # type: ignore
    
    @property
    def registration_end(self) -> date:
        date_raw = self._sheet[self._field_positions['registration_end']]
        return datetime.strptime(str(date_raw), self._date_format).date()  # noqa: DTZ007
    
    @registration_end.setter
    def registration_end(self, value: date) -> None:
        date_raw = value.strftime(self._date_format)
        self._sheet[self._field_positions['registration_end']] = date_raw
    
    @property
    def status(self) -> CourseSheetStatus:
        return CourseSheetStatus(self._sheet[self._field_positions['status']])
    
    def _validate_status(self, value: CourseSheetStatus) -> None:
        if not isinstance(value, CourseSheetStatus):
            raise TypeError("status must be of type CourseSheetStatus")
    
    @status.setter
    def status(self, value: CourseSheetStatus):
        self._validate_status(value)
        
        # adjust status of registrations based on next courseSheet status
        # disallow completion in certain cases, see below
        if value == CourseSheetStatus.IN_REVIEW:
            for registration in self.registrations:
                if registration.status != CourseSheetRegistrationStatus.OPEN:
                    continue
                if registration.member is None:
                    registration.status = CourseSheetRegistrationStatus.CANCELLED
                    continue
                registration.status = CourseSheetRegistrationStatus.IN_REVIEW     
                    
        if value == CourseSheetStatus.READY_FOR_SUBMISSION:
            for registration in self.registrations:
                if registration.status not in [CourseSheetRegistrationStatus.OPEN, CourseSheetRegistrationStatus.IN_REVIEW]:
                    continue
                if registration.member is None:
                    registration.status = CourseSheetRegistrationStatus.CANCELLED
                    continue
                if registration.status == CourseSheetRegistrationStatus.IN_REVIEW:
                    registration.status = CourseSheetRegistrationStatus.DENIED
                    continue
                # open -> ready for submission
                registration.status = CourseSheetRegistrationStatus.READY_FOR_SUBMISSION
                
            # protect sheet
            if self.status in [CourseSheetStatus.OPEN, CourseSheetStatus.IN_REVIEW]:
                self._protect_sheet()
                
        if value == CourseSheetStatus.COMPLETED:
            for registration in self.registrations:
                if registration.status in [CourseSheetRegistrationStatus.OPEN, CourseSheetRegistrationStatus.IN_REVIEW]:
                    registration.status = (CourseSheetRegistrationStatus.DENIED 
                                           if registration.member else CourseSheetRegistrationStatus.CANCELLED)
                if registration.status == CourseSheetRegistrationStatus.READY_FOR_SUBMISSION:
                    raise ValueError(f"registrations must not be in status {registration.status} to complete a course")
                
        self._sheet[self._field_positions['status']] = value.value
        
    @property
    @abstractmethod
    def teams(self) -> list[str]:
        ...
    
    @property
    @abstractmethod
    def registrations(self) -> tuple[CourseSheetRegistration, ...]:
        ...
    
    def _protect_sheet(self):
        sheet_range = GridRange(sheet_id=self._sheet.id)
        for pr in self._sheet.protected_ranges:
            if sheet_range in pr.range:
                return

        # sheet protection not yet present
        pr = ProtectedRange(range=sheet_range)
        # TODO add users and groups that can edit
        self._sheet.add_protected_range(pr)
        
class OpenCourseSheet(CourseSheet):
    
    _REGISTRATIONS_START_ROW_IDX: ClassVar[int] = 13
    
    def __init__(self, sheet: Sheet, course: Course | None = None, teams: list[str] | None = None):
        super().__init__(sheet, course, teams)
    
    @property
    def _field_positions(self) -> dict[str, str]:
        return {
            'registration_end': 'C8',
            'status': 'C9'
        }
        
    def _construct_from_template(self, applicable_members: DropDownRangeValidationRule) -> None:
        course = self.course
        teams = [] # TODO dataValidationRangeRule?
        
        details = "=HYPERLINK(\"https://bvv.volley.de/portal/core_anl.action?anleitungid=34\", \"Anleitung zur Teilnahme an Fortbildungen Online\")"
        how_to = "=HYPERLINK(\"https://sites.google.com/tsvhaunstetten.de/trainerhandbuch/mannschaft#h.h95evsjeullv\", \"siehe Trainerhandbuch\")"
        
        self._sheet['C2'] = course.type_raw
        self._sheet['C3'] = course.label
        self._sheet['C4'] = course.city
        self._sheet['C5'] = course.date_str
        self._sheet['C6'] = details
        self._sheet['C10'] = how_to
        
        self.registration_end = course.registration_end - timedelta(days=1)
        
        if self.registration_end < datetime.now().date():
            self.status = CourseSheetStatus.CLOSED
            
        # data validation for members
        validation_range = GridRange(
            sheet_id=self._sheet.id,
            start_row_idx=self._REGISTRATIONS_START_ROW_IDX,
            end_row_idx=self._sheet.row_count - 2,
            start_column_idx=OpenCourseSheetRegistration.get_field_col_idx('member'),
            end_column_idx=OpenCourseSheetRegistration.get_field_col_idx('member')
        )
        validation = DataValidation(
            sheet=self._sheet,
            range=validation_range,
            rule=applicable_members
        )
        self._sheet.apply_data_validation(validation)
        
        # TODO data validation for team names
        
        
    def _validate_status(self, value: CourseSheetStatus) -> None:
        super()._validate_status(value)
        if value in {CourseSheetStatus.IN_REVIEW, CourseSheetStatus.READY_FOR_SUBMISSION}:
            raise ValueError(f"courseSheetStatus {value} is not applicable for open courses")
        
    @property
    def teams(self) -> list[str]:
        raise NotImplementedError()
    
    @property
    def registrations(self) -> tuple[OpenCourseSheetRegistration, ...]:
        start_row_idx = self._REGISTRATIONS_START_ROW_IDX
        
        course_registrations = []
        for row_idx in range(start_row_idx, self._sheet.row_count - 2):
            registration = OpenCourseSheetRegistration(
                sheet=self._sheet,
                row_idx=row_idx
            )
            course_registrations.append(registration)
        return tuple(course_registrations)
        
        
        
class ManagedCourseSheet(CourseSheet):
    
    _REGISTRATIONS_START_ROW_IDX: ClassVar[int] = 14
    _REGISTRATION_SLOTS_PER_TEAM: ClassVar[int] = 5
    
    def __init__(self, sheet: Sheet, course: Course | None = None, applicable_members: DropDownRangeValidationRule | None = None):
        super().__init__(sheet, course, applicable_members)
    
    @property
    def _field_positions(self) -> dict[str, str]:
        return {
            'registration_end': 'C8',
            'status': 'C9'
        }
        
    def _construct_from_template(self, applicable_members: DropDownRangeValidationRule) -> None:
        course = self.course
        teams = [] # TODO, must be at least 2 teams
        
        # insert header
        details = f"=HYPERLINK(\"https://volleyball.bayern/schiri/lehrgaenge/lehrgang/lehrgang-{self.course.id}\", \"siehe Volleyball Bayern\")"
        how_to = "=HYPERLINK(\"https://sites.google.com/tsvhaunstetten.de/trainerhandbuch/mannschaft#h.h95evsjeullv\", \"siehe Trainerhandbuch\")"
        
        self._sheet['C2'] = course.type_raw
        self._sheet['C3'] = course.label
        self._sheet['C4'] = course.city
        self._sheet['C5'] = course.date_str
        self._sheet['C6'] = 3  # guaranteed places
        self._sheet['C7'] = details
        self._sheet['C11'] = how_to
        
        self.registrations_end = (datetime.now() + timedelta(weeks=4)).date()
        
        if self.registration_end < datetime.now().date():
            self.status = CourseSheetStatus.IN_REVIEW
        
        # make room for teams
        self._sheet.row_count = self._REGISTRATIONS_START_ROW_IDX + len(teams) * self._REGISTRATION_SLOTS_PER_TEAM + 1
        
        # apply data validation for member column, ready to be copied
        validation_range = GridRange(
            sheet_id=self._sheet.id,
            start_row_idx=self._REGISTRATIONS_START_ROW_IDX,
            end_row_idx=self._REGISTRATIONS_START_ROW_IDX + len(teams) * self._REGISTRATION_SLOTS_PER_TEAM - 1,
            start_column_idx=ManagedCourseSheetRegistration.get_field_col_idx('member'),
            end_column_idx=ManagedCourseSheetRegistration.get_field_col_idx('member')
        )
        validation = DataValidation(
            sheet=self._sheet,
            range=validation_range,
            rule=applicable_members
        )
        self._sheet.apply_data_validation(validation)
        self._sheet.spreadsheet.push()  # need to push before copy&paste
        
        # copy&paste team rows
        source_white = GridRange(
            sheet_id=self._sheet.id,
            start_row_idx=self._REGISTRATIONS_START_ROW_IDX,
            end_row_idx=self._REGISTRATIONS_START_ROW_IDX + self._REGISTRATION_SLOTS_PER_TEAM - 1,
            start_column_idx=0,
            end_column_idx=self._sheet.column_count - 1
        )
        source_grey = GridRange(
            sheet_id=self._sheet.id,
            start_row_idx=source_white.start_row_idx + self._REGISTRATION_SLOTS_PER_TEAM, # type: ignore
            end_row_idx=source_white.end_row_idx + self._REGISTRATION_SLOTS_PER_TEAM, # type: ignore
            start_column_idx=0,
            end_column_idx=self._sheet.column_count - 1
        )
        destinations: dict[GridRange, list[GridRange]] = {}
        for i in range(len(teams[2:])):  # template consists of 2 teams already
            destination_range = GridRange(
                sheet_id=source_white.sheet_id,
                start_row_idx=source_white.start_row_idx + (i + 2) * source_white.row_count, # type: ignore
                end_row_idx=source_white.end_row_idx + (i + 2) * source_white.row_count, # type: ignore
                start_column_idx=source_white.start_column_idx,
                end_column_idx=source_white.end_column_idx
            )
            if i % 2 == 0:
                destinations.setdefault(source_white, []).append(destination_range)
                continue
            destinations.setdefault(source_grey, []).append(destination_range)
        for source, dsts in destinations.items():
            self._sheet.copy_paste(source=source, destinations=dsts, skip_fetch=True)
        
        # insert team names
        for i, team in enumerate(teams):
            row_idx = self._REGISTRATIONS_START_ROW_IDX + (i * self._REGISTRATION_SLOTS_PER_TEAM)
            self._sheet[row_idx, ManagedCourseSheetRegistration.get_field_col_idx('team_name')] = team
        # push is done in RegistrationSpreadsheet
            
        
    def _validate_status(self, value: CourseSheetStatus) -> None:
        super()._validate_status(value)
        if value == CourseSheetStatus.CLOSED:
            raise ValueError(f"courseSheetStatus {value} is not applicable for managed courses")
        
    @property
    def teams(self) -> list[str]:
        start_row_idx = self._REGISTRATIONS_START_ROW_IDX
        column_idx = ManagedCourseSheetRegistration.get_field_col_idx('team_name')
        offset = self._REGISTRATION_SLOTS_PER_TEAM
        
        teams: list[str] = []
        for row_idx in range(start_row_idx, self._sheet.row_count - 1, offset):
            teams.append(str(self._sheet[(row_idx, column_idx)]))
        return teams
    
    @property
    def registrations(self) -> tuple[ManagedCourseSheetRegistration, ...]:
        start_row_idx = self._REGISTRATIONS_START_ROW_IDX
        team_name_column_idx = ManagedCourseSheetRegistration.get_field_col_idx('team_name')
        offset = self._REGISTRATION_SLOTS_PER_TEAM
        
        course_registrations = []
        
        for row_idx in range(start_row_idx, self._sheet.row_count - 1, offset):
            team_name = str(self._sheet[(row_idx, team_name_column_idx)])
            for i in range(offset):
                course_registration = ManagedCourseSheetRegistration(
                    sheet=self._sheet,
                    row_idx=row_idx + i,
                    team_name=team_name
                )
                course_registrations.append(course_registration)
                
        return tuple(course_registrations)
    


class OverviewSheet:
    pass

class MemberSheet:
    validation_rules: dict[str, DropDownRangeValidationRule]  # course.type_raw, applicaple members
    
    def get_applicable_members(self, course_type_raw: str) -> DropDownRangeValidationRule:
        return self.validation_rules[course_type_raw]
class RegistrationSpreadsheet:
    
    in_person_courses: list[ManagedCourseSheet]
    online_courses: list[OpenCourseSheet]
    
    member_sheet: MemberSheet
    overview_sheet: OverviewSheet
    
    _spreadsheet: Spreadsheet
    
    def __init__(self, credentials) -> None:
        pass
    
    def add_course(self, course: Course):
        # construct course sheet and add it (developerMetadata!)
        # add row to overview page
        pass
    
    def remove_course(self, course: Course):
        pass
    