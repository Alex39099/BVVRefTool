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
import os
import sys
import warnings
from datetime import datetime, timedelta

import pandas as pd

from data.BVVScalper_py import BVVScalper
from data.Config import Config
from data.CourseContainer_py import CourseContainer
from data.PersonContainer_py import PersonContainer
from data.RegistrationContainer_py import RegistrationContainer
from helper.membership_file_converter import read_club_membership_file
from mailing.MailService import Mailer
from mailing.MessageCreator import ManagementReport, ReportReason


def get_big_registrations_df(registration_container: RegistrationContainer, course_container: CourseContainer, person_container: PersonContainer, bvv_scalper: BVVScalper):
    """
    Create big registrations_df
    :param registration_container: registrations_container
    :param course_container: courses_container
    :param person_container: persons_container
    :param bvv_scalper: BVVScalper
    :return: big registrations_df with columns of registrations_df, courses_df with prefix courses_ and persons_df with prefix persons_. In addition, column 'club_membership_status'
    """
    registrations_df = registration_container.data

    # try to correct missing data in person_container
    registered_persons_unique = registrations_df[["last_name", "first_name", "birthday"]].drop_duplicates(ignore_index=True)
    merged_persons = person_container.data.merge(right=registered_persons_unique, on=["last_name", "first_name", "birthday"], how="right")
    missing_mails = merged_persons[merged_persons["mail"].isna()]
    fetched_data = bvv_scalper.get_personal_data(missing_mails[["last_name", "first_name"]].drop_duplicates(ignore_index=True))
    person_container.update(fetched_data)
    if len(fetched_data) > 0:
        # prepare for management report
        fetched_data = fetched_data[["last_name", "first_name", "birthday"]].merge(right=person_container.data, on=["last_name", "first_name", "birthday"], how="left")
        fetched_data = fetched_data[["last_name", "first_name", "birthday", "sex", "street", "postalcode", "city", "phone", "mobile", "mail", "club", "club_membership_expire"]]
        ManagementReport.instance().add_registrations(fetched_data, ReportReason.CORRECTED_MISSING_DATA)

    # retrieve person_df copy, insert club_member_status
    persons_df = person_container.data.copy()
    persons_df["club_membership_expire"] = pd.to_datetime(persons_df["club_membership_expire"])
    persons_df["club_member_status"] = (persons_df["club_membership_expire"] >= datetime.now()) | persons_df[
        "club_membership_expire"].isna()

    # create huge dataframe including all person data and course data
    persons_df = persons_df.add_prefix("person_", axis=1)
    persons_df = persons_df.rename(
        columns={"person_last_name": "last_name", "person_first_name": "first_name", "person_birthday": "birthday"})
    registrations_df = registrations_df.merge(right=persons_df, on=["last_name", "first_name", "birthday"], how="left")
    courses_df = course_container.data.add_prefix("course_", axis=1)
    courses_df = courses_df.drop("course_label", axis=1)
    registrations_df = registrations_df.merge(right=courses_df, on=["course_id"], how="left")
    return registrations_df


def manage_changed_registrations(registration_container: RegistrationContainer, course_container: CourseContainer, person_container: PersonContainer, bvv_scalper: BVVScalper):
    management_report = ManagementReport.instance()
    mailer = Mailer.instance()

    registrations_df = get_big_registrations_df(registration_container, course_container, person_container, bvv_scalper)

    # filter out any entries with person_mail = nan (this means they were not in person_df or in general we have no data!)
    registrations_no_data = registrations_df[registrations_df["person_mail"].isna()]
    registrations_no_data = registrations_no_data[registration_container.data.columns.tolist()].sort_values(registration_container.data.columns.tolist())
    management_report.add_registrations(registrations_no_data, ReportReason.MISSING_DATA)
    # remove those registrations from container (we need to preserve the index in above operations for this!)
    registration_container.data = registration_container.data.drop(registrations_no_data.index)
    registrations_df = registrations_df.drop(registrations_no_data.index)

    # filter out registrations that are pending and can still be cancelled for none club members
    registrations_to_be_cancelled = registrations_df[(~registrations_df["person_club_member_status"]) & (registrations_df["participation_status"] == "pending") & (registrations_df["course_deregistration_end"] <= datetime.now())]
    columns_of_interest_report = ["course_label", "last_name", "first_name", "birthday", "person_club_member_status"]
    registrations_to_be_cancelled = registrations_to_be_cancelled[columns_of_interest_report]
    registrations_to_be_cancelled = registrations_to_be_cancelled.sort_values(columns_of_interest_report)
    registrations_to_be_cancelled = registrations_to_be_cancelled.rename(columns={"person_club_member_status": "club_member"})
    management_report.add_registrations(registrations_to_be_cancelled, ReportReason.NOT_IN_CLUB)

    # send cancellation mail to any removed registration, treat them as cancelled for mailing
    removed_registrations = registrations_df[registrations_df["status"] == "removed"]
    mailer.send_course_cancellation(removed_registrations)
    columns_of_interest_report = ["course_label", "last_name", "first_name", "birthday"]
    removed_registrations = removed_registrations[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(removed_registrations, ReportReason.REMOVED)

    # now only work on new registrations from club_members
    changed_club_registrations = registrations_df[registrations_df["person_club_member_status"] & (registrations_df["status"].isin(["changed", "added"]))]

    # ===============================================================================================================
    #  course success
    # ===============================================================================================================

    approved_success = changed_club_registrations[(changed_club_registrations["registration_status"] == "approved") & (changed_club_registrations["participation_status"] == "passed")]
    for course_type in ["training", "refresher"]:
        mailer.send_course_success(approved_success[approved_success["course_type"] == course_type], course_type)

    # set wants_higher_license for every training-successor to false
    training_successor = approved_success[approved_success["course_type"] == "training"]
    training_successor = training_successor[["last_name", "first_name", "birthday"]]
    training_successor["wants_higher_license"] = False
    person_container.update(training_successor)

    # management report
    columns_of_interest_report = ["course_type", "course_label", "last_name", "first_name", "birthday"]
    approved_success = approved_success[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(approved_success, ReportReason.SUCCESS)

    # ===============================================================================================================
    #  course failed
    # ===============================================================================================================

    approved_failed = changed_club_registrations[(changed_club_registrations["registration_status"] == "approved") & (changed_club_registrations["participation_status"] == "failed")]

    # for "refresher" we want to reset the registration to pending if course is still doable
    approved_failed.loc[(approved_failed["course_type"] == "refresher") & (approved_failed["course_date_end"] >= datetime.now()), "participation_status"] = "pending"
    approved_failed_refresher_active = approved_failed[(approved_failed["course_type"] == "refresher") & (approved_failed["course_date_end"] >= datetime.now())]
    approved_failed_refresher_active = approved_failed_refresher_active[registration_container.data.columns.tolist()]
    registration_container.data.update(approved_failed_refresher_active)

    # now manage truly failed courses
    approved_failed = approved_failed[approved_failed["participation_status"] == "failed"]
    mailer.send_course_fail(approved_failed)
    columns_of_interest_report = ["course_type", "course_label", "course_date_start", "course_date_end", "last_name", "first_name", "birthday"]
    approved_failed = approved_failed[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(approved_failed, ReportReason.FAILED)

    # increment failed_higher_license count
    person_container.increment_data_value(approved_failed)

    # ===============================================================================================================
    #  course missed
    # ===============================================================================================================

    approved_missed = changed_club_registrations[(changed_club_registrations["registration_status"] == "approved") & (changed_club_registrations["participation_status"] == "missed")]

    # do separately for each confirmation_status
    approved_missed_confirmed = approved_missed[approved_missed["confirmation_status"] == "confirmed"]
    mailer.send_course_missed(approved_missed_confirmed)
    columns_of_interest_report = ["course_type", "course_label", "course_date_start", "course_date_end", "last_name", "first_name", "birthday"]
    approved_missed_confirmed = approved_missed_confirmed[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(approved_missed_confirmed, ReportReason.MISSED_CONFIRMED)

    # approved_missed that should have been cancelled by management
    approved_missed_bug = approved_missed[approved_missed["confirmation_status"].isin(["pending", "denied"])]
    columns_of_interest_report = registration_container.data.columns.tolist()
    approved_missed_bug = approved_missed_bug[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(approved_missed_bug, ReportReason.BUG)

    # ===============================================================================================================
    #  course registration cancelled
    # ===============================================================================================================

    cancelled = changed_club_registrations[changed_club_registrations["registration_status"] == "cancelled"]
    mailer.send_course_cancellation(cancelled)
    columns_of_interest_report = ["course_label", "last_name", "first_name", "birthday", "participation_status", "registration_status", "confirmation_status"]
    cancelled = cancelled[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(cancelled, ReportReason.CANCELLED)

    # ===============================================================================================================
    #  course registration pending
    # ===============================================================================================================

    pending = changed_club_registrations[changed_club_registrations["registration_status"].isin(["waiting", "approved"]) & (changed_club_registrations["participation_status"] == "pending")]

    # confirmation denied
    pending_confirmation_denied = pending[pending["confirmation_status"] == "denied"]
    # mailing is done when registration is cancelled
    columns_of_interest_report = ["course_label", "course_registration_end", "course_reregistration_end",
                                  "course_deregistration_end", "last_name", "first_name", "birthday",
                                  "registration_status"]
    pending_confirmation_denied = pending_confirmation_denied[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(pending_confirmation_denied, ReportReason.CONFIRMATION_DENIED)

    # confirmation confirmed
    pending_confirmation_confirmed = pending[pending["confirmation_status"] == "confirmed"]
    mailer.send_course_confirmed(pending_confirmation_confirmed)
    columns_of_interest_report = ["course_label", "course_registration_end", "course_reregistration_end",
                                  "course_deregistration_end", "last_name", "first_name", "birthday",
                                  "registration_status"]
    pending_confirmation_confirmed = pending_confirmation_confirmed[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(pending_confirmation_confirmed, ReportReason.CONFIRMATION_CONFIRMED)

    # confirmation pending
    pending_confirmation_pending = pending[pending["confirmation_status"] == "pending"]
    mailer.send_course_confirmation_request(pending_confirmation_pending)
    columns_of_interest_report = ["course_label", "course_registration_end", "course_reregistration_end",
                                  "course_deregistration_end", "last_name", "first_name", "birthday",
                                  "registration_status"]
    pending_confirmation_pending = pending_confirmation_pending[columns_of_interest_report].sort_values(columns_of_interest_report)
    management_report.add_registrations(pending_confirmation_pending, ReportReason.CONFIRMATION_PENDING)
    return


def manage_new_courses(config, new_courses: pd.DataFrame, person_container: PersonContainer):
    # only need to do management report, mailing is done by manage_registrations...

    # filter for courses we can register people to
    new_courses_active = new_courses[new_courses["registration_end"] > datetime.now()]

    # filter for courses in main districts
    new_courses_district = new_courses_active[new_courses_active["district"].isin(config.get(["general", "districts"]))]
    new_trainings_district = new_courses_district[(new_courses_district["license_category"] == "Halle") & (new_courses_district["type"] == "training")]
    new_refresher_district = new_courses_district[(new_courses_district["license_category"] == "Halle") & (new_courses_district["type"] == "refresher")]

    report = ManagementReport.instance()

    # refresher
    license_types = ["D", "C", "B"]
    for license_type in license_types:
        refresher = new_refresher_district[new_refresher_district["license_type"] == license_type]
        if len(refresher) > 0:
            candidates = person_container.get_persons_by_license(license_category="Halle", license_type=license_type,
                                                                 max_expire_offset=timedelta(days=365),
                                                                 treat_expired_as_type_dk=False)
            report.add_new_courses(refresher, candidates)

    # trainings
    license_types = ["DK", "D", "C", "BK", "B"]
    for i in range(len(license_types) - 1):
        license_type = license_types[i]
        higher_license_type = license_types[i + 1]

        trainings = new_trainings_district[new_trainings_district["license_type"] == higher_license_type]
        if len(trainings) > 0:
            candidates = person_container.get_persons_by_license(license_category="Halle", license_type=license_type,
                                                                 wants_higher_license=True)
            if len(candidates) == 0:
                candidates = person_container.get_persons_by_license(license_category="Halle", license_type="D")
                candidates = candidates.sort_values("wants_higher_license", ascending=False)
            report.add_new_courses(trainings, candidates)

    # trainings CT and CP
    trainings_ct = new_trainings_district[new_trainings_district["license_type"] == "CT"]
    if len(trainings_ct) > 0:
        candidates = person_container.get_persons_by_license(license_category="Halle", license_type="D",
                                                             wants_higher_license=True)
        if len(candidates) == 0:
            candidates = person_container.get_persons_by_license(license_category="Halle", license_type="D")
            candidates = candidates.sort_values("wants_higher_license", ascending=False)
        report.add_new_courses(trainings_ct, candidates)
    trainings_cp = new_trainings_district[new_trainings_district["license_type"] == "CP"]
    if len(trainings_cp) > 0:
        candidates = person_container.get_persons_by_license(license_category="Halle", license_type="CT",
                                                             wants_higher_license=True)
        if len(candidates) == 0:
            candidates = person_container.get_persons_by_license(license_category="Halle", license_type="D")
            candidates = candidates.sort_values("wants_higher_license", ascending=False)
        report.add_new_courses(trainings_ct, candidates)
    return


def manage_pending_courses(config: Config, registration_container: RegistrationContainer, course_container: CourseContainer, person_container: PersonContainer, bvv_scalper: BVVScalper):
    config_mail_notifications_path = ["mail_settings", "mail_notifications"]

    management_reminder_days_descending = sorted(config.get(
        config_mail_notifications_path + ["management", "course_reminder_days_before_deregistration_end"]), reverse=True)
    player_reminder_days_descending = sorted(config.get(
        config_mail_notifications_path + ["player", "course_reminder_days_before_course_start"]), reverse=True)

    management_report = ManagementReport.instance()
    mailer = Mailer.instance(config)
    big_registrations_df = get_big_registrations_df(registration_container, course_container, person_container, bvv_scalper)

    # only work on registrations with pending course and pending participation status
    pending_course_registrations = big_registrations_df[(big_registrations_df["course_date_start"] >= datetime.now()) & (big_registrations_df["participation_status"] == "pending")].copy()

    for course_id in pending_course_registrations["course_id"].unique():
        course_select = course_container.data[course_container.data["id"] == course_id]
        registrations_select = pending_course_registrations[
            pending_course_registrations["course_id"] == course_id].sort_values("waiting_position", ascending=True)

        # send reminder based on days before and previous reminder_count
        # management
        idx = int(course_select["management_reminder_count"].iloc[0])
        if idx < len(management_reminder_days_descending):  # if it fails -> no more reminder
            if (course_select["deregistration_end"].iloc[0] - datetime.now()).days < management_reminder_days_descending[idx]:
                # adjust management_reminder_count
                course_container.data.loc[course_container.data["id"] == course_id, "management_reminder_count"] += 1

                # add to management report
                registrations_select_report = registrations_select[["last_name", "first_name", "birthday", "person_club_member_status", "registration_status", "participation_status",
                                                                    "waiting_position", "confirmation_status", "person_club_team", "person_help_count", "person_failed_higher_license_count"]]
                registrations_select_report = registrations_select_report.rename(columns={
                    "person_club_member_status": "club_member",
                    "person_club_team": "club_team",
                    "person_help_count": "help_count",
                    "person_failed_higher_license_count": "failed_higher_license_count"
                })

                management_report.add_course_reminder(course_select, registrations_select_report)

        # player
        idx = int(course_select["player_reminder_count"].iloc[0])
        if idx < len(player_reminder_days_descending):  # if it fails -> no more reminder
            if (course_select["date_start"].iloc[0] - datetime.now()).days < player_reminder_days_descending[idx]:
                # adjust player_reminder_count
                course_container.data.loc[course_container.data["id"] == course_id, "player_reminder_count"] += 1

                # only send reminder to players with confirmation_status != denied
                registrations_select_players = registrations_select[registrations_select["confirmation_status"] != "denied"]

                if len(registrations_select_players) > 0:
                    # add info to management report
                    management_report.add_general_info(f"reminder mails were sent for course {course_select['label'].iloc[0]} to {len(registrations_select_players)} players.")

                    # send reminder mails to all participation pending players
                    mailer.send_course_reminder(registrations_select_players, registrations_select_players["course_type"].iloc[0])

    return


def trigger_club_potential_refs(config, person_container: PersonContainer, potential_refs):
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


def trigger_ref_search(config, person_container: PersonContainer):
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


def disable(program_path, config, containers):
    report = ManagementReport.instance().get_html_message()
    Mailer.instance().send_management_report(report)
    if report is None:
        report = "EMPTY REPORT"
    with open(os.path.join(program_path, "last_management_report.txt"), "w", encoding="utf-8") as f:
        f.write(report)

    # save data from all containers
    for container in containers:
        container.save()

    # save config
    with open(os.path.join(program_path, "config.json"), "w", encoding="utf-8") as f:
        config.save(f, ensure_ascii=False)

    sys.exit()


# TODO features:
#   - priority sorting, how do we determine priority?
#   - for players below a certain age send mail also to trainer
#   - low_team_licenses_warning for trainer
#   - go live

def main(program_path):
    logging.basicConfig(filename=os.path.join(program_path, "recent.log"), encoding="utf-8", level=logging.DEBUG)
    # suppress UserWarning from openpyxl
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    with open(os.path.join(program_path, "config.json"), encoding="utf-8") as f:
        config = Config.load(f)

    bvv_scalper = BVVScalper(config)
    Mailer.instance(config)

    management_report = ManagementReport.instance()

    course_container = CourseContainer(config, bvv_scalper)
    person_container = PersonContainer(config, bvv_scalper)
    registration_container = RegistrationContainer(config, bvv_scalper)
    containers = [course_container, person_container, registration_container]

    # load data from all containers
    for container in containers:
        container.load()

    # update data
    new_courses = course_container.update()
    person_container.update()
    registration_container.update()

    # insert course_id into registrations, assert deep course data
    registration_container.insert_course_id(course_container.data)
    course_container.assert_deep_data(list(registration_container.data["course_id"]))

    # read club_members
    settings_config_path = ["club_membership_file_settings"]
    club_members = read_club_membership_file(filepath=config.get(settings_config_path + ["file_path"]),
                                             name_converter=config.get(
                                                 settings_config_path + ["name_converter_local_to_bvv"]),
                                             date_format=config.get(settings_config_path + ["date_format"]))
    trigger_config_path = ["trigger", "club_potential_refs_update"]
    if config.get(trigger_config_path):
        logging.info(f"Trigger {trigger_config_path[1]} is active.")
        config.set(trigger_config_path, False)
        potential_refs = club_members.copy()
        trigger_club_potential_refs(config, person_container, potential_refs)

    trigger_config_path = ["trigger", "club_membership_update"]
    if config.get(trigger_config_path):
        logging.info(f"Trigger {trigger_config_path[1]} is active.")
        config.set(trigger_config_path, False)
        person_container.update_membership(
            club_members[["last_name", "first_name", "birthday", "club_membership_expire"]])
        logging.info("updated club_membership_expire")
        management_report.add_general_info("club_membership_expire has been updated.")

    trigger_config_path = ["trigger", "only_update_data"]
    if config.get(trigger_config_path):
        logging.info(f"Trigger {trigger_config_path[1]} is active.")
        config.set(trigger_config_path, False)
        management_report.add_general_info("only updated data, did not send mails.")
        disable(program_path, config, containers)

    trigger_config_path = ["trigger", "ref_search"]
    if config.get(trigger_config_path):
        logging.info(f"Trigger {trigger_config_path[1]} is active.")
        config.set(trigger_config_path, False)
        trigger_ref_search(config, person_container)

    manage_changed_registrations(registration_container, course_container, person_container, bvv_scalper)
    manage_new_courses(config, new_courses, person_container)
    manage_pending_courses(config, registration_container, course_container, person_container, bvv_scalper)

    disable(program_path, config, containers)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # print("Usage: python script.py <config_path>")
        sys_cwd = os.getcwd()
    else:
        sys_cwd = sys.argv[1]

    main(sys_cwd)
