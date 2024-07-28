#  Copyright (c) 2024. Alexander Schmid
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with this program.  If not, see <http://www.gnu.org/licenses/>.

import io
import logging
import time
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from data.Config import Config
from data.helpfunctions import remove_duplicates


class BVVScalper:

    _ENCODING = "utf-8"

    __json_section = ["bvv_settings"]
    __json_username = __json_section + ["username"]
    __json_password = __json_section + ["password"]
    __json_club_id = __json_section + ["club_id"]
    __json_bvv_date_format = __json_section + ["bvv_date_format"]
    __json_local_date_format = __json_section + ["local_date_format"]
    __json_request_delay = __json_section + ["request_delay_secs"]

    def __init__(self, config: Config):
        self.username = config.get(self.__json_username)
        self.password = config.get(self.__json_password)
        self.club_id = config.get(self.__json_club_id)
        self.bvv_date_format = config.get(self.__json_bvv_date_format)
        self.local_date_format = config.get(self.__json_local_date_format)
        self._request_delay = config.get(self.__json_request_delay)

        self.url_login = "https://bvv.volley.de/portal/core_login.action"
        self.url_logout = "https://bvv.volley.de/portal/core_logout.action"
        self.url_license_get = "https://bvv.volley.de/portal/sw_verein_scheine!browse.action?vereinsid=" + self.club_id
        self.url_license_action = "https://bvv.volley.de/portal/sw_verein_scheine.action"
        self.url_license_execute_action = "https://bvv.volley.de/portal/sw_verein_scheine!execute.action"
        self.url_person_search_get = "https://bvv.volley.de/portal/verein_verein_person!browse.action?vereinsid=" + self.club_id
        self.url_person_search_action = "https://bvv.volley.de/portal/verein_verein_personen.action"
        self.url_course_get = "https://bvv.volley.de/portal/sw_verein_lehrgaenge!browse.action?vereinsid=" + self.club_id
        self.url_course_action = "https://bvv.volley.de/portal/sw_verein_lehrgaenge.action"
        self.url_course_deep_get = "https://bvv.volley.de/portal/sw_verein_lehrgang!browse.action?vereinsid=" + self.club_id
        self.url_registration_get = "https://bvv.volley.de/portal/sw_verein_anmeldungen!browse.action?vereinsid=" + self.club_id
        self.url_registration_action = "https://bvv.volley.de/portal/sw_verein_anmeldungen.action"

        self._scalped_registrations_content = None
        self._scalped_licenses_content = None
        self._scalped_licenses_excel = None
        self._scalped_courses_content = None
        self._scalp_data()

    def _login(self):
        """
        Creates a session and logs into bvv portal homepage.
        :return: session object. IMPORTANT: Session must be closed using session.close() (or with block)
        """
        payload = {"username": self.username,
                   "password": self.password}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        session = requests.session()
        session.post(self.url_login, data=payload, headers=headers)  # login
        logging.info("BVV_SCALPER: logged into BVV site.")
        return session

    def _logout(self, session):
        response = session.post(self.url_logout)
        if response.status_code != 200:
            logging.error("BVV_SCALPER: response failed for logout")
            return None

    def _request_timing(self):
        """
        Delays the request by __request_delay, should be executed before the actual request.
        :return: None
        """
        time.sleep(self._request_delay)
        return

    def _scalp_data(self):
        with self._login() as session:
            self._request_timing()
            self._scalped_registrations_content = self._scalp_current_registrations(session)
            self._scalped_licenses_content = self._scalp_licenses(session)
            self._scalped_licenses_excel = self._scalp_licenses_excel(session)
            self._scalped_courses_content = self._scalp_courses(session)
            self._request_timing()
            self._logout(session)
        return

    # =================================================================================================================
    #      REGISTRATIONS
    # =================================================================================================================

    def get_registrations(self, start: datetime = None, end: datetime = None):
        """
        Get the current registrations.
        :return: df with columns course_label, last_name, first_name, birthday,
        registration_status (passed/failed/missed), participation_status (approved/cancelled/waiting), waiting_position (int/nan)
        """
        if end is None:
            end = datetime.now()
            end = datetime(year=end.year + 1, month=end.month, day=end.day)
        if start is None:
            start = datetime.now()
            start = datetime(year=start.year - 1, month=start.month, day=start.day)

        df = self._fetch_current_registrations(start=start, end=end)

        # converting birthday to datetime
        df['birthday'] = pd.to_datetime(df['birthday'], format=self.bvv_date_format)

        # transform values of participation_status
        participation_mapping = {
            'erfolgreich teilgenommen': 'passed',
            'teilgenommen': 'passed',
            'nicht erfolgreich teilgenommen': 'failed',
            'nicht teilgenommen': 'missed',
            'unbekannt': 'pending'
        }
        df['participation_status'] = df['participation_status'].map(participation_mapping)

        # transform values of registration_status
        registration_mapping = {
            'zugelassen': 'approved',
            'storniert (kostenfrei)': 'cancelled',
            'ohne Anmeldung': 'approved'
        }
        df['registration_status'] = df['registration_status'].replace(registration_mapping)

        # Extract waiting_position for "Warteliste (digit)" patterns
        df['waiting_position'] = df['registration_status'].str.extract(r'Warteliste \((\d+)\)')
        # Replace "Warteliste (digit)" with "waiting" in registration_status
        df.loc[df['registration_status'].str.contains('Warteliste'), 'registration_status'] = 'waiting'

        # Convert waiting_position to numeric (float or int), conversion fail or missing values will result in NaN
        df['waiting_position'] = pd.to_numeric(df['waiting_position'], errors='coerce')

        # Any non-waiting has waiting_position 0
        df.loc[df['registration_status'] != 'waiting', "waiting_position"] = 0
        return df

    def _fetch_current_registrations(self, start: datetime, end: datetime):
        soup = BeautifulSoup(self._scalped_registrations_content, 'html.parser')
        rows = soup.find('table').find_all('tr')

        if rows and "keine Anmeldungen für Lehrgänge im angegebenen Zeitraum gefunden" in rows[0].get_text():
            logging.warning(f"BVV_SCALPER: fetch_current_registrations could not find any courses between {start} and {end}")
            return None

        data = []
        current_course_label = None

        for row in rows:
            columns = row.find_all('td')
            headers = row.find_all('th')

            if headers and 2 < len(headers) < 6:
                current_course_label = headers[1].get_text()
                continue

            if columns and len(columns) >= 6:
                entry = {
                    "course_label": current_course_label,
                    "last_name": columns[0].get_text(),
                    "first_name": columns[1].get_text(),
                    "birthday": columns[2].get_text(),
                    "registration_status": columns[4].get_text(),
                    "participation_status": columns[5].get_text()
                }
                data.append(entry)
        logging.info(f"BVV_SCALPER: fetched current registrations between {start} and {end} (count = {len(data)}")
        return pd.DataFrame(data)

    def _scalp_current_registrations(self, session, start=None, end=None):
        self._request_timing()
        if start is None:
            start = datetime.now()
            start = datetime(year=start.year - 1, month=start.month, day=start.day)

        if end is None:
            end = datetime.now()
            end = datetime(year=end.year + 1, month=end.month, day=end.day)

        if start > end:
            raise ValueError("start must be earlier than end")

        form_data = {
            "vereinsid": self.club_id,
            "von": start.strftime(self.bvv_date_format),
            "bis": end.strftime(self.bvv_date_format)
        }
        response = session.post(self.url_registration_action, data=form_data)

        if response.status_code != 200:
            logging.error("BVV_SCALPER: response failed for fetch_current_registrations ")
            return None

        response.encoding = BVVScalper._ENCODING
        return response.text

    # =================================================================================================================
    #      LICENSES
    # =================================================================================================================

    def get_licenses(self):
        """
        Gets the current licenses on the BVV site with removed duplicates, replaced umlaute in last_name, first_name and dates transformed to datetime objs
        :return: df with columns last_name, first_name, birthday, phone, mobile, mail, street, postalcode, city,
        license_category, license_type, license_id, license_bvv_id, license_since, license_expire, club
        """
        direct_licenses = self._fetch_licenses()
        excel_licenses = self._download_licenses()

        # remove possible duplicates based on completeness
        direct_licenses = remove_duplicates(direct_licenses, subset=["last_name", "first_name"])
        excel_licenses = remove_duplicates(excel_licenses, subset=["last_name", "first_name", "birthday"])

        # transform umlaute in names
        # direct_licenses = replace_umlaute(direct_licenses, columns=["last_name", "first_name"])
        # excel_licenses = replace_umlaute(excel_licenses, columns=["last_name", "first_name"])

        # transform dates
        date_columns = ["license_since", "license_expire"]
        for column in date_columns:
            direct_licenses[column] = pd.to_datetime(direct_licenses[column], format=self.bvv_date_format)
            excel_licenses[column] = pd.to_datetime(excel_licenses[column], format=self.local_date_format)
        excel_licenses["birthday"] = pd.to_datetime(excel_licenses["birthday"], format=self.local_date_format)

        # not specifying on-param will lead to intersection of columns
        merged_df = excel_licenses.merge(right=direct_licenses, how="left", suffixes=("", "_right"))

        # only keep data from Excel and license_bvv_id
        cols_to_drop = [col for col in merged_df.columns if col.endswith('_right')]
        df = merged_df.drop(cols_to_drop, axis=1)

        # transform sex
        sex_mapping = {
            'männlich': 'm',
            'weiblich': 'f'
        }
        df['sex'] = df['sex'].map(sex_mapping)

        # merge phone numbers
        df.loc[df['phone'].isna(), 'phone'] = df['phone2']
        df = df.drop('phone2', axis=1)

        return df

    def _fetch_licenses(self):
        content = self._scalped_licenses_content
        soup = BeautifulSoup(content, 'html.parser')

        # Find the form by ID
        form = soup.find('form', {'id': 'sw_verein_lehrgangsanmeldunginit'})

        # Find the nested table within the form
        tables = form.find_all('table')
        table = tables[-1]

        # Get all rows except the header
        rows = table.find_all('tr')[2:]

        # List to store all entries
        entries = []

        for row in rows:
            cells = row.find_all('td')

            # Get license_bvv_id from the checkbox input's name attribute
            element = row.find('input', type='checkbox')
            if element:
                internal_license_id = element['name'].split('[')[1].split(']')[0]
            else:
                # Handle the case where the element was not found
                logging.error(f"BVV_SCALPER: Did not find a checkbox for {row}")
                continue

            # license_parts "Halle C", "Beach C"...
            license_parts = cells[3].get_text(strip=True).split(" ", 1)
            license_category = license_parts[0]  # Halle/Beach
            license_type = license_parts[1]  # level

            # Get the text from each cell and store it in a dictionary
            entry = {
                'last_name': cells[1].get_text(strip=True),
                'first_name': cells[2].get_text(strip=True),
                'license_category': license_category,
                'license_type': license_type,
                'license_id': cells[4].get_text(strip=True),
                'license_bvv_id': internal_license_id,
                'license_since': cells[5].get_text(strip=True),
                'license_expire': cells[6].get_text(strip=True),
                'club': cells[7].get_text(strip=True)
            }

            # Add the entry to the entries list
            entries.append(entry)

        # Create a DataFrame from the entries list
        df = pd.DataFrame(entries)
        return df

    def _scalp_licenses(self, session):
        self._request_timing()
        form_data = {
            "vereinsid": self.club_id,
            "typid": "-2",
            "gueltigkeitid": "1",
            "personenfilterid": "0",
            "sortertypid": "1"
        }
        session.post(self.url_license_action, data=form_data)

        response = session.get(self.url_license_get)
        if response.status_code != 200:
            logging.error("BVV_SCALPER: response failed for load_licenses")
            return None

        response.encoding = BVVScalper._ENCODING
        return response.text

    def _download_licenses(self):
        df = self._scalped_licenses_excel

        # rename columns
        new_names = {
            "Name": "last_name",
            "Vorname": "first_name",
            "Geburtsdatum": "birthday",
            "Geschlecht": "sex",
            "Strasse": "street",
            "PLZ": "postalcode",
            "Ort": "city",
            "Telefon (p)": "phone",
            "Telefon (d)": "phone2",
            "Mobil": "mobile",
            "E-Mail": "mail",
            "Kategorie": "license_category",
            "Typ": "license_type",
            "Nr": "license_id",
            "Ab": "license_since",
            "Bis": "license_expire",
            "Verein": "club",
        }

        df.rename(columns=new_names, inplace=True)
        return df

    def _scalp_licenses_excel(self, session):
        self._request_timing()
        url = self.url_license_execute_action
        data = {
            'vereinsid': self.club_id,
            'resulttype': 'excel'
        }
        response = session.post(url, data=data)
        if response.status_code != 200:
            logging.error("BVV_SCALPER: response failed for download_licenses")
            return None

        with io.BytesIO(response.content) as fh:
            return pd.io.excel.read_excel(fh, sheet_name=0)

    # =================================================================================================================
    #      COURSES
    # =================================================================================================================

    def get_courses(self):
        courses = self._fetch_courses()
        courses = self._transform_course_df(courses)
        logging.debug("BVV_SCALPER: get_courses successfully.")
        return courses

    def get_deep_course_info(self, lids: Union[list[str], str]):
        courses = self._fetch_deep_course_info(lids)
        courses = self._transform_course_df(courses)

        # transform additional date columns
        date_columns = ["deregistration_end", "reregistration_end"]
        for column in date_columns:
            courses[column] = pd.to_datetime(courses[column], format=self.bvv_date_format)

        logging.debug(f"BVV_SCALPER: get_deep_courses_info successfully for lids = {lids}")

        return courses

    def _fetch_courses(self):
        content = self._scalped_courses_content
        soup = BeautifulSoup(content, 'html.parser')

        # Find the course table by its class name
        table = soup.find('table', {'class': 'portaltable'})

        data_list = []

        # Loop through each row in the table
        current_section = None
        for row in table.find_all('tr'):
            # Check if row is a section header
            section = row.find('div', {'class': 'sectionheader'})
            if section:
                current_section = section.text
                continue

            # Extract data from each cell in the row
            cells = row.find_all('td')
            if len(cells) > 1:  # Ignore rows that don't have multiple cells (like headers)
                course_type, license_category, license_type = self.get_types_from_course_section(current_section)
                date_start, date_end = self.get_date_bounds(cells[1].text)
                register_start, register_end = self.get_date_bounds(cells[4].text)

                # Extract the 'lid' from the last cell's 'href' attribute
                lid_link = cells[8].find('a')['href']
                lid = lid_link.split('lid=')[1].split('&')[0]

                data_dict = {
                    'id': lid,
                    'district': cells[0].text,
                    'label': cells[2].text,
                    'type': course_type,
                    'date_start': date_start,
                    'date_end': date_end,
                    'license_category': license_category,
                    'license_type': license_type,
                    'city': cells[3].text,
                    'registration_start': register_start,
                    'registration_end': register_end,
                    'free_space': cells[5].text,
                    'granted_space': self.get_granted_space(cells[6].text),
                    'waiting_count': cells[7].text
                }

                data_list.append(data_dict)

        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(data_list)

        logging.info("BVV_SCALPER: fetched courses")
        return df

    def _scalp_courses(self, session):
        self._request_timing()
        # check checkbox to get all courses
        form_data = {
            "vereinsid": self.club_id,
            "alle": "true",
            "_checkbox_alle": "true"
        }
        form_action_url = self.url_course_action
        session.post(form_action_url, data=form_data)

        # Fetch page data
        response = session.get(self.url_course_get)
        if response.status_code != 200:  # status_code 200 == success
            return []

        response.encoding = BVVScalper._ENCODING
        return response.text

    def _fetch_deep_course_info(self, lids: Union[list[str], str]):
        """
        Fetches deep course info
        :param lids: str or list of strs
        :return: df
        """
        if isinstance(lids, str):
            lids = [lids]
        self._request_timing()

        contents = []
        with self._login() as session:
            for lid in lids:
                url = self.url_course_deep_get + f"&lid={lid}"
                response = session.get(url)
                response.encoding = self._ENCODING
                contents.append(response.text)
            self._logout(session)

        res = []
        for i in range(len(lids)):
            lid = lids[i]
            content = contents[i]
            info = self.get_deep_course_info_from_content(lid, content)
            logging.info("BVV_SCALPER: fetched deep course info of " + info["label"] + "(id = " + lid + ")")
            res.append(info)

        return pd.DataFrame(res)

    def _transform_course_df(self, courses):
        # drop anything other than Ausbildung/Fortbildung
        courses = courses.loc[courses["type"].isin(["Ausbildung", "Fortbildung"])].copy()

        # transform date columns
        date_columns = ["date_start", "date_end", "registration_start", "registration_end"]
        for column in date_columns:
            courses.loc[:, column] = pd.to_datetime(courses[column], format=self.bvv_date_format)

        # transform numeric columns
        numeric_columns = ["free_space", "granted_space", "waiting_count"]
        for column in numeric_columns:
            courses.loc[:, column] = pd.to_numeric(courses[column], errors="coerce")

        # rename certain values
        courses.loc[courses["type"] == "Ausbildung", "type"] = "training"
        courses.loc[courses["type"] == "Fortbildung", "type"] = "refresher"

        return courses

    @staticmethod
    def get_deep_course_info_from_content(lid: str, content):
        soup = BeautifulSoup(content, 'html.parser')
        fetched_info = {}

        course_table = soup.find_all('table')[0]  # Assuming the first table contains additional information for course
        for row in course_table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 2:
                key = cells[0].text.strip().replace(":", "")
                value = cells[1].text.strip()
                fetched_info[key] = value

        contact_table = soup.find_all('table')[1]  # Assuming the second table contains contact information
        for row in contact_table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 2:
                key = cells[0].text.strip().replace(":", "")
                value = cells[1].text.strip()
                fetched_info["contact_" + key] = value

        space_table = soup.find_all('table')[2]
        for row in space_table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 2:
                key = cells[0].text.strip().replace(":", "")
                value = cells[1].text.strip()
                fetched_info["space_" + key] = value

        course_type, license_category, license_type = BVVScalper.get_types_from_course_section(fetched_info['Typ'])
        register_start, register_end = BVVScalper.get_date_bounds(fetched_info['Anmeldezeitraum'])
        date_start, date_end = BVVScalper.get_date_bounds(fetched_info['Datum'])

        info = {
            'id': lid,
            'district': fetched_info['Bereich'],
            'label': fetched_info['Name'],
            'type': course_type,
            'date_start': date_start,
            'date_end': date_end,
            'license_category': license_category,
            'license_type': license_type,
            'city': fetched_info['Ort'],
            'registration_start': register_start,
            'registration_end': register_end,
            'free_space': fetched_info['space_freie Plätze'],
            'granted_space': BVVScalper.get_granted_space(fetched_info['space_davon sofort verfügbar']),
            'waiting_count': fetched_info['space_auf Warteliste'],
            'deregistration_end': fetched_info['Abmeldeschluss'],
            'reregistration_end': fetched_info['Ummeldeschluss'],
            'address': fetched_info['Anschrift'],
            'remark': fetched_info['Bemerkung'],
            'contact_name': fetched_info['contact_Name'],
            'contact_mail': fetched_info['contact_Email'],
            'contact_phone': fetched_info['contact_Telefon'],
            'contact_mobile': fetched_info['contact_Mobil']
        }
        return info

    @staticmethod
    def get_granted_space(fetched_granted_space):
        return fetched_granted_space if fetched_granted_space.isnumeric() else '0'

    @staticmethod
    def get_types_from_course_section(section_name):
        """
        Get course_type, license_category and license_type from a course's section_name
        :param section_name: the fetched section_name
        :return: course_type (Ausbildung/Fortbildung), license_category (Halle/Beach/Trainer), license_type (B, BK, C, CP, CT, D)
        """
        split = section_name.split("-")

        if len(split) > 1:
            # license_category (Halle, Beach)
            if section_name.startswith("Beach"):
                license_category = "Beach"
            elif "Trainer" in section_name:
                logging.debug(f"BVV_SCALPER: section name {section_name} led to hard coded trainer course.")
                return "Regelkunde", "Trainer", "C"  # stupid exception "Regelkunde C-Trainer / Regeltest
            else:
                license_category = "Halle"

            # license_type (B, BK, C, CT, CP, D, Jugend)
            license_type = split[0].removeprefix("Beach ")  # remove Beach prefix if present
            if section_name.endswith("(Theorie)"):
                license_type += "T"
            elif section_name.endswith("(Praxis)"):
                license_type += "P"
            elif section_name.endswith("(Theorie und Praxis") or section_name.endswith("(Theorie + Praxis)"):
                license_type += ""

            # course_type (Ausbildung/Fortbildung)
            if "Ausbildung" in section_name:
                course_type = "Ausbildung"
            elif "Fortbildung" in section_name:
                course_type = "Fortbildung"
            else:
                course_type = "UNDEFINED"

            logging.debug(f"BVV_SCALPER: section name {section_name} led to: course_type = {course_type}, license_category = {license_category}, license_type = {license_type}.")
            return course_type, license_category, license_type
        return None

    @staticmethod
    def get_date_bounds(period: str):
        """
        Processes a period like string (e.g. "01.01.2023", "01.01. - 31.12.2023" or "01.01.2022 - 31.12.2023")
        :param period: the period string
        :return: start, end as strings
        """
        period_split = period.split(' - ')
        if len(period_split) == 1:
            return period.strip(), period.strip()
        else:
            date_str1 = period_split[0]
            date_str2 = period_split[1]

            # Split each date string into components
            components1 = date_str1.split('.')
            components2 = date_str2.split('.')

            # If the year is not present in the first date, add it from the second date
            if len(components1) == 3 and not components1[2]:
                components1.remove("")
                components1.append(components2[-1])

            # Join the components back into date strings
            new_date_str1 = '.'.join(components1).strip()
            new_date_str2 = '.'.join(components2).strip()

            return new_date_str1, new_date_str2

    # =================================================================================================================
    #      PERSONAL DATA
    # =================================================================================================================

    def get_personal_data(self, names: pd.DataFrame):
        names = names.copy()
        bvv_user_ids = self._fetch_bvv_ids_by_names(names)
        personal_data_dicts = self._fetch_personal_data_by_bvv_ids(bvv_user_ids)

        personal_data = pd.DataFrame(personal_data_dicts)

        if len(personal_data) == 0:
            return personal_data

        # transform data
        personal_data["sex"] = personal_data["sex"].apply(self.get_sex)
        personal_data["phone"] = personal_data.apply(lambda row: self.get_phone(row["phone_g"], row["phone_p"]), axis=1)
        personal_data["birthday"] = pd.to_datetime(personal_data["birthday"], format=self.bvv_date_format)

        # replace empty strings with nan
        personal_data = personal_data.replace("", np.nan)

        return personal_data

    def _fetch_personal_data_by_bvv_ids(self, bvv_user_ids: list[str]):
        contents = []

        self._request_timing()
        with self._login() as session:
            for bvv_user_id in bvv_user_ids:
                # get personal info with bvv_user_id
                get_url = self.url_person_search_get + "&userid=" + str(bvv_user_id)
                response = session.get(get_url)
                if response.status_code != 200:
                    logging.error(f"BVV_SCALPER: response failed for get_personal_info (bvv_user_id = {bvv_user_id})")
                    continue

                response.encoding = self._ENCODING
                contents.append(response.text)
            self._logout(session)

        res_data = []
        for content in contents:
            soup = BeautifulSoup(content, 'html.parser')

            data = {
                "bvv_user_id": bvv_user_id,
                "last_name": soup.find('label', {'id': 'user_name'}).get_text(),
                "first_name": soup.find('label', {'id': 'user_vorname'}).get_text(),
                "sex": soup.find('label', {
                    'id': 'user_geschlecht_getGeschlechtBezeichnung__GeschlechtFormatterA__'}).get_text(),
                "birthday": soup.find('label', {
                    'id': 'user_geschlecht_getGeschlechtBezeichnung__GeschlechtFormatterA__'}).find_next('td').find_next(
                    'td').get_text(),
                "mail": soup.find('label', {'id': 'user_email'}).get_text(),
                "street": soup.find('label', {'id': 'user_strasse'}).get_text(),
                "postalcode": soup.find('label', {'id': 'user_plz'}).get_text(),
                "city": soup.find('label', {'id': 'user_ort'}).get_text(),
                "country": soup.find('label', {'id': 'user_land'}).get_text(),
                "phone_p": soup.find('label', {'id': 'user_telefon_p'}).get_text(),
                "phone_g": soup.find('label', {'id': 'user_telefon_g'}).get_text(),
                "mobile": soup.find('label', {'id': 'user_mobil'}).get_text()
            }
            res_data.append(data)
            logging.info("BVV_SCALPER: fetched personal info for " + data["last_name"] + ", " + data["first_name"])
        return res_data

    def _fetch_bvv_ids_by_names(self, names: pd.DataFrame):
        names = names[["last_name", "first_name"]]
        if names.isna().any().any():
            raise ValueError("last_name and/or first_name has nan values!")

        id_contents = []
        bvv_user_ids = []

        self._request_timing()
        with self._login() as session:
            for _, row in names.iterrows():

                form_action_url = self.url_person_search_action
                form_data = {
                    "vereinsid": self.club_id,
                    "operation": "suche",
                    "name": row["last_name"],
                    "vorname": row["first_name"]
                }
                response = session.post(form_action_url, data=form_data)
                if response.status_code != 200:
                    logging.error(f"BVV_SCALPER: response failed for fetching user_id with {form_data['name']}, {form_data['vorname']}")
                    continue

                response.encoding = self._ENCODING
                id_contents.append(response.text)
            self._logout(session)

        for id_content in id_contents:
            soup = BeautifulSoup(id_content, 'html.parser')
            try:
                # Find the 'sectionheader' div and then find the subsequent 'portaltable' table
                table = soup.find('div', class_='sectionheader').find_next('table', class_='portaltable')

                # Find the 'a' tag within the table and extract the 'href' attribute
                href = table.find('a')['href']

                # Extract the userid from the 'href' attribute using string splitting methods
                bvv_user_id = href.split('userid=')[1].split('&')[0]
                bvv_user_ids.append(bvv_user_id)
            except Exception as err:
                logging.error(
                    f"BVV_SCALPER: An error occurred while fetching bvv_user_id get_personal_info: {err=}, {type(err)=}. "
                    f"Most likely {form_data['name']}, {form_data['vorname']} does not exist on the BVV page.")
                continue

        return bvv_user_ids

    @staticmethod
    def get_sex(bvv_sex):
        if bvv_sex == "männlich":
            return "m"
        elif bvv_sex == "weiblich":
            return "f"
        else:
            return np.nan

    @staticmethod
    def get_phone(phone_g, phone_p):
        if phone_p is not None and phone_p != "":
            return phone_p
        elif phone_g is not None and phone_g != "":
            return phone_g
        else:
            return np.nan