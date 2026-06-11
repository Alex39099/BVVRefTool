#  Copyright (c) 2026. Alexander Schmid
import io
import logging
import re
import time
import warnings
from dataclasses import dataclass, InitVar
from datetime import datetime, date
from typing import Any, BinaryIO, cast
from urllib.parse import urlparse, parse_qs

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from AppConfig import AppConfig
from manager.Data import RefLicenseCategory, Course, RefLicenseType, Registration, RegistrationStatus, \
    ParticipationStatus, CourseType, RefLicense, PersonIdentity, Referee, Participant, GrantableLicenseCategory, \
    GrantableLicenseType, GrantableLicense


class BVVSession(requests.Session):
    def __init__(self, username: str, password: str, url_login: str, url_logout: str, min_throttle: float = 5.0):
        super().__init__()
        self.username = username
        self.password = password
        self.url_login = url_login
        self.url_logout = url_logout
        self.min_throttle = min_throttle  # seconds
        self._last_request_time = None

        # Retry configuration
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1,
            allowed_methods=["GET", "POST"],
            status_forcelist=[500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

        # Disable connection reuse
        self.headers.update({"Connection": "close"})

    def request(self, method, url, **kwargs): # type: ignore
        # enforce timeout
        if "timeout" not in kwargs:
            kwargs["timeout"] = 15

        # throttling
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            wait = self.min_throttle - elapsed
            if wait > 0:
                logging.debug(f"BVV_SESSION: delaying next request by {wait:.2f} seconds...")
                time.sleep(wait)

        try:
            response = super().request(method, url, **kwargs)
        except requests.exceptions.ConnectionError:
            logging.warning("BVV_SESSION: connection error, retry handled by adapter")
            raise

        self._last_request_time = time.time()

        # detect silent session expiry
        if response.status_code != 200 or "core_login" in response.url.lower():
            raise RuntimeError("Session expired or invalid response")
        return response

    def __enter__(self):
        payload = {"username": self.username, "password": self.password}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = self.post(self.url_login, data=payload, headers=headers)
        if response.status_code != 200:
            logging.error("BVV_SCALPER: login failed")
            raise RuntimeError("Login failed")

        if "core_login" in response.url.lower():
            raise RuntimeError("Login rejected (still on login page)")

        logging.info("BVV_SCALPER: logged in")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb): # type: ignore
        try:
            response = self.post(self.url_logout)
            if response.status_code != 200:
                logging.error("BVV_SCALPER: logout failed")
            else:
                logging.info("BVV_SCALPER: logged out")
        except Exception as e:
            logging.exception(f"BVV_SCALPER: logout exception: {e}")
        finally:
            self.close()
        return False  # propagate exceptions from the block


@dataclass
class BVVScraper:

    credentials: InitVar[tuple[str, str]]
    club_id: str = "555"  # TSV Haunstetten
    bvv_date_format: str = "%d.%m.%Y"
    request_min_throttle: float = 5.0

    @classmethod
    def from_config(cls, config: AppConfig) -> "BVVScraper":
        return cls(
            credentials=(config.bvv.username, config.bvv.password),
            club_id=config.bvv.club_id
        )

    def __post_init__(self, credentials: tuple[str, str]):
        self.username = credentials[0]
        self.password = credentials[1]

        self.url_login = "https://bvv.volley.de/portal/core_login.action"
        self.url_logout = "https://bvv.volley.de/portal/core_logout.action"
        self.url_license_get = "https://bvv.volley.de/portal/sw_verein_scheine!browse.action?vereinsid=" + self.club_id
        self.url_license_action = "https://bvv.volley.de/portal/sw_verein_scheine.action"
        self.url_license_excel_action = "https://bvv.volley.de/portal/sw_verein_scheine!execute.action"
        # self.url_person_search_get = "https://bvv.volley.de/portal/verein_verein_person!browse.action?vereinsid=" + self.club_id
        # self.url_person_search_action = "https://bvv.volley.de/portal/verein_verein_personen.action"
        self.url_course_get = "https://bvv.volley.de/portal/sw_verein_lehrgaenge!browse.action?vereinsid=" + self.club_id
        self.url_course_action = "https://bvv.volley.de/portal/sw_verein_lehrgaenge.action"
        self.url_course_deep_get = "https://bvv.volley.de/portal/sw_verein_lehrgang!browse.action?vereinsid=" + self.club_id
        # self.url_registration_get = "https://bvv.volley.de/portal/sw_verein_anmeldungen!browse.action?vereinsid=" + self.club_id
        self.url_registration_action = "https://bvv.volley.de/portal/sw_verein_anmeldungen.action"

    def get_session(self) -> BVVSession:
        """
        Get a session to BVV that automatically logs in and out on session start and close.
        <p>In addition, throttling is configured as per the self configurations
        :return: session object
        """
        return BVVSession(self.username, self.password, self.url_login, self.url_logout, self.request_min_throttle)

    def scrape_registrations(self, session: BVVSession, start: date | None = None, end: date | None = None) -> bytes:
        """
        Scrape all registrations within the given time period
        :param session: the BVVSession
        :param start: start date for the search period, defaults to 1 year before
        :param end: end date for the search period, defaults to 1 year after start date
        :return: content of the BVV response
        """
        if start is None:
            start = datetime.today()
            start = date(year=start.year - 1, month=start.month, day=start.day)
        if end is None:
            end = date(year=start.year + 1, month=start.month, day=start.day)

        for name, value in (("start date", start), ("end date", end)):
            if not isinstance(value, date):
                raise TypeError(f"{name} must be datetime.date")

        if start > end:
            raise ValueError("start must be earlier than end")

        data = {
            "vereinsid": self.club_id,
            "von": start.strftime(self.bvv_date_format),
            "bis": end.strftime(self.bvv_date_format)
        }
        response = session.post(self.url_registration_action, data=data)
        response.raise_for_status()
        return response.content

    def scrape_licenses(self, session: BVVSession) -> bytes:
        """
        Scrape all licenses
        :param session: the BVVSession
        :return: content of the BVV response
        """
        data = {
            "vereinsid": self.club_id,
            "typid": "-2",
            "gueltigkeitid": "1",
            "personenfilterid": "0",
            "sortertypid": "1"
        }
        response = session.post(self.url_license_action, data=data)
        response.raise_for_status()
        response = session.get(self.url_license_get)  # get call with data does not work
        response.raise_for_status()
        return response.content

    def scrape_licenses_excel(self, session: BVVSession) -> bytes:
        """
        Scrape an Excel file containing all licenses
        <p>The contents are on sheet 0.
        :param session: the BVVSession
        :return: content of the BVV response
        """
        data = {
            'vereinsid': self.club_id,
            'resulttype': 'excel'  # only type excel seems to work
        }
        response = session.post(self.url_license_excel_action, data=data)
        response.raise_for_status()
        return response.content

    def scrape_courses(self, session: BVVSession) -> bytes:
        """
        Scrape all courses (from all districts)
        :param session: the BVVSession
        :return: content of the BVV response
        """
        data = {
            "vereinsid": self.club_id,
            "alle": "true",
            "_checkbox_alle": "true"
        }
        response = session.post(self.url_course_action, data=data)
        response.raise_for_status()
        response = session.get(self.url_course_get)  # get call with data does not work
        response.raise_for_status()
        return response.content

    def scrape_courses_deep(self, course_ids: list[str] | str, session: BVVSession) -> dict[str, bytes]:
        """
        Scrape deep course information from each individual course page
        :param course_ids: either a single or list of course_ids
        :param session: the BVVSession
        :return: dict with key course_id and value response's content
        """
        if isinstance(course_ids, str):
            course_ids = [course_ids]

        contents = {}
        for course_id in course_ids:
            url = f"{self.url_course_deep_get}&lid={course_id}"
            response = session.get(url)
            response.raise_for_status()
            contents[course_id] = response.content

        return contents

# ====================================================================================================================
# ====================================================================================================================
# ====================================================================================================================


def parse_courses_from_html(raw_html: bytes) -> list[dict[str, str]]:
    soup = BeautifulSoup(raw_html, 'html.parser')

    # Find course table by class name
    table = soup.find('table', {'class': 'portaltable'})
    
    if table is None:
        raise ValueError("Could not find course table in HTML")        

    courses = []

    # Loop through each row within the table
    current_section = None
    for row in table.find_all('tr'):
        # Check if row is a section header and continue
        section = row.find('div', {'class': 'sectionheader'})
        if section:
            current_section = section.text
            continue

        # row has course data
        cells = row.find_all('td')
        if len(cells) > 1:
            if not current_section:
                raise ValueError("Parser error: course row encountered before section header was parsed")
            
            # get lid
            lid_raw = cells[8].find('a', href=True)
            lid = str(lid_raw['href']).split('lid=')[1].split('&')[0] if lid_raw and 'lid=' in lid_raw['href'] else None
            
            course_data = {
                'Bereich': cells[0].text,
                'Datum': cells[1].text,
                'Bezeichnung': cells[2].text,
                'Ort': cells[3].text,
                'Anmeldezeitraum': cells[4].text,
                'freie Plätze': cells[5].text,
                'davon sofort verfügbar': cells[6].text,
                'Warteliste': cells[7].text,
                'Id': lid,
                'Typ': current_section
            }
            courses.append(course_data)

    return courses


def parse_deep_course_from_html(course_id: str, raw_html: bytes) -> dict[str, str]:
    soup = BeautifulSoup(raw_html, 'html.parser')
    fetched_info = {}

    tables = soup.find_all('table')

    def extract_text_preserve_breaks(tag) -> str:
        for br in tag.find_all("br"):
            br.replace_with("\n")
        return tag.get_text()

    def parse_key_value_table(table):
        result = {}
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).replace(":", "")
                value = extract_text_preserve_breaks(cells[1]).strip()
                result[key] = value
        return result

    # course info
    fetched_info.update(parse_key_value_table(tables[0]))

    # contact info
    contact_info = parse_key_value_table(tables[1])
    if contact_info:
        fetched_info['Ansprechpartner'] = contact_info

    # space info
    fetched_info.update(parse_key_value_table(tables[2]))

    return {'Id': course_id, **fetched_info}


def parse_licenses_from_html(raw_html: bytes) -> list[dict[str, Any]]:
    raise NotImplementedError(raw_html)


def parse_licenses_from_excel(excel: bytes | BinaryIO) -> list[dict[str, Any]]:
    data_columns = ['Name', 'Vorname', 'E-Mail', 'Kategorie', 'Typ', 'Nr']
    date_columns = ['Geburtsdatum', 'Ab', 'Bis']

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style",
            category=UserWarning
        )

        fh = io.BytesIO(excel) if isinstance(excel, (bytes, bytearray)) else excel
        df = pd.read_excel(fh, usecols=data_columns + date_columns)

    for date_column in date_columns:
        df[date_column] = df[date_column].dt.date

    return cast(list[dict[str, Any]], df.to_dict(orient='records'))


def parse_registrations_from_html(raw_html: bytes) -> list[dict[str, Any]]:
    soup = BeautifulSoup(raw_html, 'html.parser')
    table = soup.find('table')
    if not table:
        raise ValueError("Could not find registrations table in HTML")
    
    rows = table.find_all('tr')
    if rows and "keine Anmeldungen für Lehrgänge im angegebenen Zeitraum gefunden" in rows[0].get_text():
        return []

    registrations = []
    current_course_info = {}

    for row in rows:
        columns = row.find_all('td')
        headers = row.find_all('th')

        # filter for "blue" header
        if headers and 2 < len(headers) < 6:
            current_course_info = {
                'Typ': headers[0].get_text(),
                'Name': headers[1].get_text(),
                'Ort': headers[2].get_text(),
                'Datum': headers[3].get_text()
            }
            continue

        if columns and len(columns) >= 6:
            if not current_course_info:
                raise ValueError("Parser error: registration row encountered before course header was parsed")

            aid = None
            link_tag = columns[-1].find('a', href=True)
            if link_tag:
                query = urlparse(str(link_tag['href'])).query
                aid = parse_qs(query).get('aid', [None])[0]

            if not aid:
                raise ValueError("Parse error: aid could not be parsed")

            entry = {
                'Id': aid,
                'Name': columns[0].get_text(),
                'Vorname': columns[1].get_text(),
                'Geburtsdatum': columns[2].get_text(),
                'Anmeldestatus': columns[4].get_text(),
                'Teilnahmestatus': columns[5].get_text(),
                'Kurs': current_course_info
            }
            registrations.append(entry)

    return registrations

# ====================================================================================================================
# ====================================================================================================================
# ====================================================================================================================


def normalize_course(raw: dict[str, str]) -> Course:
    label = raw.get('Bezeichnung', raw.get('Name'))
    if not label:
        raise ValueError("label not found in raw data, provide either 'Name' or 'Bezeichnung'")

    type_raw = raw['Typ']
    if any(x in type_raw for x in ['Ausbildung', 'Regelkunde']):
        type = CourseType.AUSBILDUNG
    elif 'Fortbildung' in type_raw:
        type = CourseType.FORTBILDUNG
    else:
        raise ValueError(f"course type {type_raw} is not valid.")

    start, end = parse_date_period(raw['Datum'])
    assert start is not None and end is not None, "could not parse start and end date from raw data"
    reg_start, reg_end = parse_date_period(raw['Anmeldezeitraum'])
    assert reg_start is not None and reg_end is not None, "could not parse registration start and end date from raw data"

    # license_category
    if 'Beach' in type_raw:
        license_category = GrantableLicenseCategory.BEACH
    elif 'Trainer' in type_raw:
        license_category = GrantableLicenseCategory.TRAINER
    else:
        license_category = GrantableLicenseCategory.HALLE

    # license_type
    type_raw_split = type_raw.split("-")
    if len(type_raw_split) > 1:
        license_type_raw = type_raw_split[0].split(" ")[-1]  # remove prefixes if present
        # parse multiple license types, e.g. "C/D"
        license_type_raw_split = license_type_raw.split("/")

        # edge case C Theorie/C Praxis
        type_suffix = ""
        if "Praxis" in type_raw and "Theorie" not in type_raw:
            type_suffix = "P"
        elif "Theorie" in type_raw and "Praxis" not in type_raw:
            type_suffix = "T"

        if type_suffix:
            license_type_raw_split = [t + type_suffix for t in license_type_raw_split]

        grantable_licenses = frozenset(
            GrantableLicense(
                category=license_category,
                type=GrantableLicenseType(license_type)
            ) for license_type in license_type_raw_split
        )
    else:
        raise ValueError(f"could not parse license_type_raw from Typ: {type_raw}")

    free_space = normalize_space(raw['freie Plätze'])
    granted_space = normalize_space(raw['davon sofort verfügbar'])
    waiting_count = normalize_space(raw.get('auf Warteliste', raw['Warteliste']))

    # deep info
    deregister_end = raw.get('Abmeldeschluss')
    deregister_end = datetime.strptime(deregister_end, '%d.%m.%Y').date() if deregister_end else None
    reregister_end = raw.get('Ummeldeschluss')
    reregister_end = datetime.strptime(reregister_end, '%d.%m.%Y').date() if reregister_end else None
    # contact_raw = raw.get('Ansprechpartner')

    return Course(
        id=raw['Id'],
        district=raw['Bereich'],
        label=label,
        type=type,
        date_start=start,
        date_end=end,
        grantable_licenses=grantable_licenses,
        registration_start=reg_start,
        registration_end=reg_end,
        free_space=free_space,
        granted_space=granted_space,
        waiting_count=waiting_count,
        city=raw['Ort'],
        deregistration_end=deregister_end,
        reregistration_end=reregister_end,
        address=raw.get('Anschrift'),
        remark=raw.get('Bemerkung')
    )


def normalize_registration(raw: dict[str, Any]) -> Registration:
    identity = PersonIdentity(
        last_name=raw['Name'],
        first_name=raw['Vorname'],
        birth_date=datetime.strptime(raw['Geburtsdatum'], '%d.%m.%Y').date()
    )
    participant = Participant(identity=identity)

    # registration_status
    raw_registration_status = raw['Anmeldestatus']
    if 'Warteliste' in raw_registration_status:
        registration_status = RegistrationStatus.WAITING
        # parse waiting position
        match = re.search(r'Warteliste \((\d+)\)', raw_registration_status)
        if not match:
            raise ValueError(f"could not parse waiting_position from raw_registration_status {raw_registration_status}")
        waiting_position = int(match.group(1))
    else:
        mapping = {
            'zugelassen': 'APPROVED',
            'storniert (kostenfrei)': 'CANCELLED',
            'ohne Anmeldung': 'APPROVED'
        }
        registration_status = RegistrationStatus(mapping.get(raw_registration_status))
        waiting_position = 0

    # participation_status
    mapping = {
        'erfolgreich teilgenommen': 'PASSED',
        'teilgenommen': 'PASSED',
        'nicht erfolgreich teilgenommen': 'FAILED',
        'nicht teilgenommen': 'MISSED',
        'unbekannt': 'PENDING'
    }
    participation_status = ParticipationStatus(mapping.get(raw['Teilnahmestatus']))

    return Registration(
        id=raw['Id'],
        course_label=raw['Kurs']['Name'],
        participant=participant,
        registration_status=RegistrationStatus(registration_status),
        participation_status=ParticipationStatus(participation_status),
        waiting_position=waiting_position
    )


def normalize_license(raw: dict[str, Any], excel: bool = True) -> Referee:
    if not excel:
        raise NotImplementedError("licenses from html are not implemented")

    # Jugend license is represented as 'J' in Excel file
    type_raw = raw['Typ']
    if type_raw == 'J':
        type_raw = 'JUGEND'

    # excel row normalization; dates are already in correct type
    ref_license = RefLicense(
        category=RefLicenseCategory(raw['Kategorie']),
        type=RefLicenseType(type_raw),
        id=raw['Nr'],
        start_date=raw['Ab'],
        end_date=raw['Bis']
    )

    identity = PersonIdentity(
        first_name=raw['Vorname'],
        last_name=raw['Name'],
        birth_date=raw['Geburtsdatum']
    )

    return Referee(
        identity=identity,
        license=ref_license
    )


def normalize_space(raw_granted_space: str) -> int:
    return int(raw_granted_space) if raw_granted_space.isnumeric() else 0


def parse_date_period(period: str | None) -> tuple[date | None, date | None]:
    """
    Processes a period like string (e.g. "01.01.2023", "01.01. - 31.12.2023" or "01.01.2022 - 31.12.2023")
    :param period: the period string. None or empty strings result in (None, None)
    :return: start, end as date objects or (None, None). True date strings will result in (same_date, same_date)
    """
    if not period:
        return None, None

    period_split = period.split(' - ')

    if len(period_split) == 1:
        parsed_date = datetime.strptime(period_split[0], '%d.%m.%Y').date()
        return parsed_date, parsed_date

    # Split each date string into components
    components1 = period_split[0].split('.')
    components2 = period_split[1].split('.')

    # If the year is not present in the first date, add it from the second date
    if len(components1) == 3 and not components1[2]:
        components1.remove("")
        components1.append(components2[-1])

    # parse dates from components
    date1 = datetime.strptime('.'.join(components1).strip(), '%d.%m.%Y').date()
    date2 = datetime.strptime('.'.join(components2).strip(), '%d.%m.%Y').date()

    return date1, date2
