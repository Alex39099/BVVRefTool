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

from data.BVVScalper_py import BVVScalper
from data.Config import Config
from data.CourseContainer_py import CourseContainer
from data.PersonContainer_py import PersonContainer
from data.RegistrationContainer_py import RegistrationContainer
from helper.membership_file_converter import read_club_membership_file
from mailing.MailService import Mailer
from mailing.MessageCreator import ManagementReport
from helper.managing_data import manage_changed_registrations, manage_pending_courses, manage_new_courses
from helper.managing_trigger import trigger_club_potential_refs, trigger_ref_search, trigger_refresher_pending


def disable(program_path, config, containers):
    report = ManagementReport.instance().get_html_message()
    Mailer.instance().send_management_report(report)
    if report is None:
        report = "EMPTY REPORT"
    with open(os.path.join(program_path, "last_management_report.txt"), "w", encoding="utf-8") as f:
        f.write(report)

    # save data from all containers
    for container in containers.values():
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

    containers = {
        "course": course_container,
        "person": person_container,
        "registration": registration_container
    }

    # load data from all containers
    for container in containers.values():
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

    trigger_config_path = ["trigger", "refresher_pending_in_report"]
    if config.get(trigger_config_path):
        logging.info(f"Trigger {trigger_config_path[1]} is active.")
        config.set(trigger_config_path, False)
        trigger_refresher_pending(config, course_container, person_container)

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
