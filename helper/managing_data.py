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

from datetime import datetime, timedelta

import pandas as pd

from data.BVVScalper_py import BVVScalper
from data.Config import Config
from data.CourseContainer_py import CourseContainer
from data.PersonContainer_py import PersonContainer
from data.RegistrationContainer_py import RegistrationContainer
from mailing.MailService import Mailer
from mailing.MessageCreator import ManagementReport, ReportReason


def get_big_registrations_df(registration_container: RegistrationContainer, course_container: CourseContainer, person_container: PersonContainer, bvv_scalper: BVVScalper):
    """
    Create big registrations_df
    :param registration_container: registrations_container
    :param course_container: courses_container
    :param person_container: persons_container
    :param bvv_scalper: BVVScalper
    :return: big registrations_df with columns of registrations_df, courses_df with prefix courses_ and persons_df with prefix persons_. In addition, column 'club_member_status'
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

    # treat online refresher differently
    pending_refresher_online = pending[(pending["course_type"] == "refresher") & (pending["city"] == "Online")]
    mailer.send_course_confirmed(pending_refresher_online, refresher_online=True)
    management_report.add_general_info(f"{len(pending_refresher_online)} people received online refresher mails.")

    pending = pending[(pending["course_type"] != "refresher") | (pending["city"] != "Online")]

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
            # ignore wants_higher_license if no one wants it.
            if len(candidates) == 0:
                candidates = person_container.get_persons_by_license(license_category="Halle", license_type=license_type)

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
    pending_course_registrations = big_registrations_df[(big_registrations_df["course_date_start"] >= datetime.now())
                                                        & (big_registrations_df["registration_status"] != "cancelled")
                                                        & (big_registrations_df["participation_status"] == "pending")].copy()

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
                    management_report.add_general_info(f"reminder mails were sent for course {course_select['label'].iloc[0]} ({course_select['city'].iloc[0]}) to {len(registrations_select_players)} players.")

                    # send reminder mails to all participation pending players
                    mailer.send_course_reminder(registrations_select_players, registrations_select_players["course_type"].iloc[0])

    return
