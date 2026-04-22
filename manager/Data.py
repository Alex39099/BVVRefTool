#  Copyright (c) 2026. Alexander Schmid
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from html import escape


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

    @staticmethod
    def fmt_date(value: datetime | date | None) -> str | None:
        if isinstance(value, datetime) or isinstance(value, date):
            return value.strftime('%d.%m.%Y')
        return None

    def __str__(self) -> str:
        licenses = "\n".join(
            f"    - {lic.type.value}/{lic.category.value}"
            for lic in self.grantable_licenses
        ) or "    - none"

        return (
            f"Course {self.id} — {self.label}\n"
            f"{'-' * 50}\n"
            f"District:            {self.district}\n"
            f"Type:                {self.type}\n"
            f"Location:            {self.city}\n"
            f"Date:                {self.fmt_date(self.date_start)} → {self.fmt_date(self.date_end)}\n"
            f"Registration:        {self.registration_start} → {self.registration_end}\n"
            f"Re-registration end: {self.reregistration_end or '-'}\n"
            f"Deregistration end:  {self.deregistration_end or '-'}\n"
            f"\n"
            f"Capacity:\n"
            f"  Free:              {self.free_space}\n"
            f"  Granted:           {self.granted_space}\n"
            f"  Waiting:           {self.waiting_count}\n"
            f"\n"
            f"Grantable Licenses:\n{licenses}\n"
            f"\n"
            f"Address:             {self.address or '-'}\n"
            f"Remark:              {self.remark or '-'}"
        )

    def to_html(self) -> str:
        def fmt(value):
            if isinstance(value, datetime) or isinstance(value, date):
                value = self.fmt_date(value)
            return escape(str(value)) if value is not None else "-"

        licenses = "".join(
            f"<li>{escape(lic.type.value)}/{escape(lic.category.value)}</li>"
            for lic in self.grantable_licenses
        ) or "<li>-</li>"

        return f"""
    <table border="1" cellspacing="0" cellpadding="6">
        <tr><th colspan="2">Course {fmt(self.id)} — {fmt(self.label)}</th></tr>

        <tr><td>District</td><td>{fmt(self.district)}</td></tr>
        <tr><td>Type</td><td>{fmt(self.type)}</td></tr>
        <tr><td>Location</td><td>{fmt(self.city)}</td></tr>

        <tr><td>Date</td><td>{fmt(self.date_start)} → {fmt(self.date_end)}</td></tr>
        <tr><td>Registration</td><td>{fmt(self.registration_start)} → {fmt(self.registration_end)}</td></tr>
        <tr><td>Re-registration end</td><td>{fmt(self.reregistration_end)}</td></tr>
        <tr><td>Deregistration end</td><td>{fmt(self.deregistration_end)}</td></tr>

        <tr><td>Free space</td><td>{fmt(self.free_space)}</td></tr>
        <tr><td>Granted space</td><td>{fmt(self.granted_space)}</td></tr>
        <tr><td>Waiting count</td><td>{fmt(self.waiting_count)}</td></tr>

        <tr>
            <td>Grantable Licenses</td>
            <td><ul>{licenses}</ul></td>
        </tr>

        <tr><td>Address</td><td>{fmt(self.address)}</td></tr>
        <tr><td>Remark</td><td>{fmt(self.remark)}</td></tr>
    </table>
    """.strip()


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
