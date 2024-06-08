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
from datetime import datetime

import pandas as pd

from data.Config import Config
from data.CourseContainer_py import CourseContainer
from data.PersonContainer_py import PersonContainer
from helper.managing_data import manage_new_courses
from mailing.MailService import Mailer
from mailing.MessageCreator import ManagementReport


def trigger_club_potential_refs(config: Config, person_container: PersonContainer, potential_refs):
    if config.get(["club_potential_refs_settings", "remove_existing_potential_refs"]):
        person_df = person_container.data
        person_df = person_df[person_df["license_type"] != "DK"].copy()
        person_container.update(person_df, keep_persons=False)
        logging.info(f"removed all license_type == DK refs.")

    today = datetime.today()
    potential_refs["age"] = potential_refs['birthday'].apply(
        lambda x: today.year - x.year - ((today.month, today.day) < (x.month, x.day))
    )
    # filter by age
    potential_refs = potential_refs[
        (potential_refs["age"] >= config.get(["club_potential_refs_settings", "minimum_age"]))
        & (potential_refs["age"] <= config.get(["club_potential_refs_settings", "maximum_age"]))]

    # only select new people
    merged_persons = potential_refs.merge(right=person_container.data, on=["last_name", "first_name", "birthday"],
                                          how="outer", suffixes=["", "_right"], indicator=True)
    merged_persons = merged_persons.loc[merged_persons["_merge"] == "left_only"]
    merged_persons = merged_persons.drop(
        columns=[column for column in merged_persons.columns if "_right" in column])  # drop merged columns again
    potential_refs = merged_persons.drop(columns=["_merge"])

    # add potential refs to personContainer
    person_container.update(potential_refs)
    logging.info(f"added {len(potential_refs)} potential refs to the dataset.")
    ManagementReport.instance().add_general_info(f"added {len(potential_refs)} potential refs to the dataset.")
    return


def trigger_ref_search(config: Config, person_container: PersonContainer):
    settings = config.get(["ref_search_settings"])

    possible_license_categories = ["Halle", "Beach"]
    possible_license_types = ["D", "C", "BK", "B"]

    license_category = settings["license_category"]
    license_type = settings["license_type"]

    if (license_category in possible_license_categories) and (license_type in possible_license_types):
        license_types = [possible_license_types[i] for i in range(possible_license_types.index(license_type))]
        potential_refs = person_container.get_persons_by_license(license_category=license_category, license_type=license_types)

        # filter out players from requesting team
        potential_refs = potential_refs[potential_refs["club_team"] != settings["club_team"]]

        for key, value in settings.items():
            if "date" not in key:
                potential_refs["request_" + key] = value

        potential_refs["request_date"] = pd.to_datetime(settings["date"], format=settings["date_format"])

        Mailer.instance().send_ref_request(potential_refs)
        ManagementReport.instance().add_general_info(f"sent ref-requests to {len(potential_refs)} potential refs ({license_category}, {license_types}).")
    else:
        ManagementReport.instance().add_to_do("could not send ref-requests because license_category or license_type is not possible. Check settings and try again!")


def trigger_refresher_pending(config: Config, course_container: CourseContainer, person_container: PersonContainer):
    courses = course_container.data
    courses = courses[courses["district"].isin(config.get(["general", "districts"])) & (courses["type"] == "refresher") & (courses["license_category"] == "Halle") & (courses["date_end"] > datetime.now())]
    manage_new_courses(config, courses, person_container)
    return
