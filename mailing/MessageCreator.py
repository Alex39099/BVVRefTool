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

import logging
from enum import unique, IntEnum

import pandas as pd


@unique
class ReportReason(IntEnum):
    BUG = 0,
    MISSING_DATA = 1,
    CORRECTED_MISSING_DATA = 2,
    NOT_IN_CLUB = 3,
    MISSED_CONFIRMED = 4,
    FAILED = 5,
    REMOVED = 6,
    SUCCESS = 7,
    CANCELLED = 8,
    CONFIRMATION_CONFIRMED = 9,
    CONFIRMATION_DENIED = 10,
    CONFIRMATION_PENDING = 11,


class ManagementReport:
    # singleton!
    _instance = None

    _TD_ALIGNMENT = "center"

    @staticmethod
    def instance():
        if ManagementReport._instance is None:
            ManagementReport._instance = ManagementReport()
        return ManagementReport._instance

    def __init__(self):
        self.general_info = []
        self.to_do = []
        self.new_courses = []
        self.suggested_persons_new_courses = []  # parallel to new_courses!
        self.course_reminder = []
        self.course_reminder_registrations = []  # parallel to course_reminder!
        self.registrations = [pd.DataFrame() for _ in range(len(ReportReason.__members__))]
        return

    def has_content(self):
        return len(self.new_courses) != 0 or len(self.general_info) != 0 or len(self.to_do) != 0 or len(self.course_reminder) != 0

    def add_to_do(self, to_do):
        self.to_do.append(to_do)
        return self

    def add_general_info(self, info):
        self.general_info.append(info)
        return self

    def add_new_courses(self, courses: pd.DataFrame, persons: pd.DataFrame):
        """
        Add courses to the management report.
        :param courses: the courses to add.
        :param persons: persons belonging to the courses
        :return: self for chain-building
        """
        if len(courses) > 0:
            if len(self.new_courses) == 0:
                self.add_to_do("We have at least one new course (see below).")
            courses = courses.reset_index(drop=True).copy()
            persons = persons.reset_index(drop=True).copy()
            self.new_courses.append(courses)
            self.suggested_persons_new_courses.append(persons)
        return self

    def add_course_reminder(self, courses: pd.DataFrame, registrations: pd.DataFrame):
        """
        Add course reminder to the management report
        :param courses: the courses to remind for.
        :param registrations: registrations belonging to the given courses.
        :return: self for chain-building
        """
        if len(courses) + len(registrations) > 0:
            if len(self.course_reminder) == 0:
                self.add_to_do("There is at least one course reminder in the report (see below).")
            courses = courses.reset_index(drop=True).copy()
            registrations = registrations.reset_index(drop=True).copy()
            self.course_reminder.append(courses)
            self.course_reminder_registrations.append(registrations)
        return self

    def _add_registration(self, registrations: pd.DataFrame, reason: ReportReason):
        self.registrations[reason.value] = pd.concat([self.registrations[reason.value], registrations], ignore_index=True)

    def add_registrations(self, registrations: pd.DataFrame, reason: ReportReason):
        if len(registrations) == 0:
            return
        if reason == ReportReason.BUG:
            self.add_to_do(f"We have {len(registrations)} bugged registrations.")
            self._add_registration(registrations, reason)
        elif reason == ReportReason.MISSING_DATA:
            self.add_to_do(f"We had {len(registrations)} registrations in total with missing data (see persons below)! Try to add potential refs via the config trigger or add them manually to persons.csv! We will look at them again next run...")
            self._add_registration(registrations.drop_duplicates(subset=["last_name", "first_name", "birthday"]), reason)
        elif reason == ReportReason.CORRECTED_MISSING_DATA:
            self.add_to_do(f"We needed to correct {len(registrations)} missing data (see persons below) via BVV data! Check if they are actual club members and/or if we need to add them to name converter!")
            self._add_registration(registrations.drop_duplicates(subset=["last_name", "first_name", "birthday"]), reason)
        elif reason == ReportReason.NOT_IN_CLUB:
            self.add_to_do(f"We need to cancel {len(registrations)} registrations (see below).")
            self._add_registration(registrations, reason)
        elif reason == ReportReason.MISSED_CONFIRMED or reason == ReportReason.FAILED:
            self._add_registration(registrations, reason)
        elif reason == ReportReason.REMOVED:
            self.add_general_info(f"{len(registrations)} registrations got removed due to re-registrations. Cancellation mails have been sent.")
        elif reason == ReportReason.SUCCESS:
            self.add_general_info(f"{len(registrations)} players successfully participated in a course.")
        elif reason == ReportReason.CANCELLED:
            self.add_general_info(f"{len(registrations)} registrations got cancelled.")
        elif reason == ReportReason.CONFIRMATION_CONFIRMED:
            self.add_general_info(f"{len(registrations)} registrations got confirmed.")
        elif reason == ReportReason.CONFIRMATION_PENDING:
            self.add_general_info(f"{len(registrations)} (changed) registrations have pending confirmation_status. (see below)")
            self._add_registration(registrations, reason)
        elif reason == ReportReason.CONFIRMATION_DENIED:
            self.add_general_info(f"{len(registrations)} registrations were confirmation denied (see below)")
            self._add_registration(registrations, reason)

        else:
            raise ValueError(f"ReportReason {reason.name} is not supported.")

    def get_html_message(self):
        if not self.has_content():
            logging.info("ManagementReport: Did not build a report because it had no content.")
            return None

        msg = """\
        <html>
        <style>
        table, th, td {
        border:1px solid black;
        }
        tr:nth-child(even) {
            background-color: #D6EEEE;
        }
        </style>
        <body>
        """

        if len(self.to_do) > 0:
            msg += "<h1>TO-DO</h1>"
            for i in range(len(self.to_do)):
                msg += f"<p>{self.to_do[i]}</p>"

        if len(self.general_info) > 0:
            msg += "<h1>General Information</h1>"
            for i in range(len(self.general_info)):
                msg += f"<p>{self.general_info[i]}</p>"

        if len(self.course_reminder) > 0:
            msg += "<h1>Course Reminder</h1>"
            for i in range(len(self.course_reminder)):
                courses = self.course_reminder[i]

                earliest_date = courses[["reregistration_end", "deregistration_end"]].min(axis=1).min()
                sub_msg = f"<h2>Course Reminder: We have until {earliest_date:%Y-%m-%d}</h2>"
                sub_msg += f"<p>The registrations of the following courses can all be modified until {earliest_date:%Y-%m-%d}. Be aware that registrations that are listed below with MISSING DATA are NOT listed here!</p>"

                stripped_courses = courses[["district", "label", "city", "date_start", "date_end", "registration_end", "reregistration_end", "deregistration_end", "free_space", "granted_space", "waiting_count"]]
                sub_msg += stripped_courses.to_html(index=False, float_format='{:.0f}'.format).replace('<td>', f'<td align="{self._TD_ALIGNMENT}">')

                sub_msg += "<p>Current registrations:</p>"
                persons = self.course_reminder_registrations[i]
                sub_msg += persons.to_html(index=False, float_format='{:.0f}'.format).replace('<td>', f'<td align="{self._TD_ALIGNMENT}">')

                msg += sub_msg

        sub_msg = "<h1>Important Registration-Changes</h1>"
        include_sub_msg = False
        for reason in ReportReason:
            df = self.registrations[reason.value]
            if not df.empty:
                include_sub_msg = True
                sub_msg += f"<p>Registrations: {reason.name}</p>"
                sub_msg += df.to_html(index=False, float_format='{:.0f}'.format).replace('<td>', f'<td align="{self._TD_ALIGNMENT}">')

        if include_sub_msg:
            msg += sub_msg

        if len(self.new_courses) != 0:
            msg += "<h1>New Courses</h1>"
            for i in range(len(self.new_courses)):
                courses = self.new_courses[i]

                type = courses.loc[0, "type"]
                license_category = courses.loc[0, "license_category"]
                license_type = courses.loc[0, "license_type"]
                sub_msg = f"<h2>New {type}: {license_category} {license_type}</h2>"

                sub_msg += "<p>Available courses:</p>"
                stripped_courses = courses[
                    ["district", "label", "city", "date_start", "date_end", "registration_start", "registration_end", "free_space", "granted_space", "waiting_count"]]
                sub_msg += stripped_courses.to_html(index=False, float_format='{:.0f}'.format).replace('<td>', f'<td align="{self._TD_ALIGNMENT}">')

                sub_msg += "<p>Suggested registrations:</p>"
                persons = self.suggested_persons_new_courses[i]
                stripped_persons = persons[
                    ["last_name", "first_name", "license_category", "license_type", "license_expire", "club", "club_membership_expire", "club_team", "help_count", "failed_higher_license_count", "wants_higher_license"]]
                sub_msg += stripped_persons.to_html(index=False, float_format='{:.0f}'.format).replace('<td>', f'<td align="{self._TD_ALIGNMENT}">')

                msg += sub_msg

        return msg + "</body></html>"
