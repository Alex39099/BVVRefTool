#  Copyright (c) 2026. Alexander Schmid
from dataclasses import dataclass
from datetime import date
from enum import StrEnum


class UpperStrEnum(StrEnum):
    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            value = value.upper()
            for member in cls:
                if member.value == value:
                    return member
        return None


class RefLicenseCategory(UpperStrEnum):
    HALLE = 'HALLE'
    BEACH = 'BEACH'


class RefLicenseType(UpperStrEnum):
    JUGEND = 'JUGEND'
    D = 'D'
    C = 'C'
    BK = 'BK'
    B = 'B'
    AK = 'AK'
    A = 'A'


@dataclass(frozen=True)
class RefLicense:
    category: RefLicenseCategory
    type: RefLicenseType
    id: str
    start_date: date
    end_date: date

    def __post_init__(self):
        if self.end_date < self.start_date:
            raise ValueError(f"end_date {self.end_date} cannot be before start_date {self.start_date}")

    def is_active(self, today: date = date.today()):
        return self.start_date <= today <= self.end_date

    def is_renewable(self, today: date = date.today()):
        return self.end_date.year - 1 <= today.year <= self.end_date.year


@dataclass(frozen=True)
class PersonIdentity:
    first_name: str
    last_name: str
    birth_date: date


@dataclass(frozen=True)
class Referee:
    identity: PersonIdentity
    license: RefLicense


@dataclass(frozen=True)
class Participant:
    identity: PersonIdentity


class CourseType(UpperStrEnum):
    AUSBILDUNG = 'AUSBILDUNG'
    FORTBILDUNG = 'FORTBILDUNG'


class GrantableLicenseType(UpperStrEnum):
    JUGEND = 'JUGEND'
    AUSBILDER = 'AUSBILDER'
    D = 'D'
    C = 'C'
    CT = 'CT'
    CP = 'CP'
    BK = 'BK'
    B = 'B'
    AK = 'AK'
    A = 'A'


class GrantableLicenseCategory(UpperStrEnum):
    HALLE = 'HALLE'
    BEACH = 'BEACH'
    TRAINER = 'TRAINER'


@dataclass(frozen=True)
class GrantableLicense:
    type: GrantableLicenseType
    category: GrantableLicenseCategory


@dataclass(frozen=True)
class Course:
    id: str
    district: str
    label: str
    type: str
    date_start: date
    date_end: date
    grantable_licenses: frozenset[GrantableLicense]
    registration_start: date
    registration_end: date
    free_space: int
    granted_space: int
    waiting_count: int
    city: str
    reregistration_end: date | None = None
    deregistration_end: date | None = None
    address: str | None = None
    remark: str | None = None

    def has_open_registrations(self, today: date = date.today()):
        return self.registration_start <= today <= self.registration_end


class RegistrationStatus(UpperStrEnum):
    APPROVED = 'APPROVED'
    CANCELLED = 'CANCELLED'
    WAITING = 'WAITING'


class ParticipationStatus(UpperStrEnum):
    PASSED = 'PASSED'
    FAILED = 'FAILED'
    MISSED = 'MISSED'
    PENDING = 'PENDING'


@dataclass
class Registration:
    id: str
    participant: Participant
    course_label: str
    registration_status: str
    participation_status: str
    waiting_position: int

    def __post_init__(self):
        if self.waiting_position < 0:
            raise ValueError("waiting_position must not be negative")
        elif self.waiting_position == 0 and self.registration_status == RegistrationStatus.WAITING:
            raise ValueError("waiting_position must not be 0 for RegistrationStatus.WAITING")
