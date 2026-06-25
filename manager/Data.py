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
    birth_date: date | None = None
    id: str | None = None


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
    type: CourseType
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

        date_str = (f"{self.fmt_date(self.date_start)} → {self.fmt_date(self.date_end)}"
                    if self.date_start != self.date_end else self.fmt_date(self.date_start))

        return (
            f"Course {self.id} — {self.label}\n"
            f"{'-' * 50}\n"
            f"District:            {self.district}\n"
            f"Type:                {self.type}\n"
            f"Location:            {self.city}\n"
            f"Date:                {date_str}\n"
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

    def to_html(self, skip_empty: bool = False, course_url: str | None = None, labels: dict | None = None) -> str:
        if labels is None:
            labels = {
                "district": "District",
                "type": "Type",
                "location": "Location",
                "date": "Date",
                "registration": "Registration",
                "reregistration_end": "Re-registration end",
                "deregistration_end": "Deregistration end",
                "free_space": "Free space",
                "granted_space": "Granted space",
                "waiting_count": "Waiting count",
                "grantable_licenses": "Grantable Licenses",
                "address": "Address",
                "remark": "Remark",
                "portal_link": "Open in Portal"
            }

        def fmt(value):
            if isinstance(value, datetime) or isinstance(value, date):
                value = self.fmt_date(value)
            return escape(str(value)) if value is not None else "-"

        def is_empty(value) -> bool:
            return value is None or str(value).strip() == ""

        def row(label, value):
            if skip_empty and is_empty(value):
                return ""
            return f"<tr><td>{label}</td><td>{fmt(value)}</td></tr>"

        licenses = "".join(
            f"<li>{escape(lic.type.value)}/{escape(lic.category.value)}</li>"
            for lic in self.grantable_licenses
        ) or "<li>-</li>"

        date_str = (f"{fmt(self.date_start)} → {fmt(self.date_end)}"
                    if self.date_start != self.date_end else fmt(self.date_start))

        return f"""
        <table border="1" cellspacing="0" cellpadding="6">
         <tr><th colspan="2">Course {fmt(self.id)} — {fmt(self.label)}</th></tr>

         {row(labels["district"], self.district)}
         {row(labels["type"], self.type)}
         {row(labels["location"], self.city)}

         <tr><td>{labels["date"]}</td><td>{date_str}</td></tr>
         <tr><td>{labels["registration"]}</td><td>{fmt(self.registration_start)} → {fmt(self.registration_end)}</td></tr>
         {row(labels["reregistration_end"], self.reregistration_end)}
         {row(labels["deregistration_end"], self.deregistration_end)}

         {row(labels["free_space"], self.free_space)}
         {row(labels["granted_space"], self.granted_space)}
         {row(labels["waiting_count"], self.waiting_count)}

         <tr>
         <td>{labels["grantable_licenses"]}</td>
         <td><ul>{licenses}</ul></td>
         </tr>

         {row(labels["address"], self.address)}
         {row(labels["remark"], self.remark)}
         {f'<tr><td colspan="2"><a href="{escape(course_url)}">{labels["portal_link"]}</a></td></tr>' if course_url else ""}
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
    registration_status: RegistrationStatus
    participation_status: ParticipationStatus
    waiting_position: int

    def __post_init__(self):
        if self.waiting_position < 0:
            raise ValueError("waiting_position must not be negative")
        elif self.waiting_position == 0 and self.registration_status == RegistrationStatus.WAITING:
            raise ValueError("waiting_position must not be 0 for RegistrationStatus.WAITING")
