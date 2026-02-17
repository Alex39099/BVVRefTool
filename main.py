#  Copyright (c) 2024-2026. Alexander Schmid
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
import email
import imaplib
import json
import logging
import os
import sys
import warnings

from helper.Mailing import SMTPSettings, MailConstructor, Mailer
from manager.BVVTools import BVVScraper, parse_courses_from_html, normalize_course
from manager.DiffLayer import DiffLayer, ChangeEventType
from manager.Storage import SnapshotRepository, SnapshotSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def scrape_and_save_data(credentials: tuple[str, str], db_path: str):
    scraper = BVVScraper(credentials)
    snapshot_rep = SnapshotRepository(db_path)

    run_id = snapshot_rep.create_run()
    scraped_data = {}

    # scrape data at once so we only have one login
    with scraper.get_session() as session:
        scraped_data[SnapshotSource.BVV_COURSES] = scraper.scrape_courses(session)
        scraped_data[SnapshotSource.BVV_REGISTRATIONS] = scraper.scrape_registrations(session)
        scraped_data[SnapshotSource.BVV_LICENSES_EXCEL] = scraper.scrape_licenses_excel(session)
    logger.info(f"all data was scraped for run_id {run_id}")

    # save data in repo
    for k, v in scraped_data:
        snapshot_rep.save_snapshot(run_id, source=k, raw_data=v)
        logger.info(f"saved snapshot: run_id = {run_id}, {k}")
    logger.info(f"all data was saved for run_id = {run_id}")

    return scraped_data


def send_new_course_notification(db_path: str, smtp_settings: SMTPSettings):
    snapshot_repo = SnapshotRepository(db_path)
    recent_course_snapshots = snapshot_repo.get_recent_snapshots(source=SnapshotSource.BVV_COURSES, limit=2)
    parsed_courses = [parse_courses_from_html(snapshot.raw_data) for snapshot in recent_course_snapshots]
    normalized_courses = [[normalize_course(parsed_course) for parsed_course in parsed_courses[i]]
                          for i in range(len(parsed_courses))]

    if len(normalized_courses) == 0:
        raise ValueError("no data available, fetch data first")

    latest_courses = normalized_courses[0]
    previous_courses = []
    if len(normalized_courses) > 1:
        previous_courses = normalized_courses[1]

    diff_layer = DiffLayer(key_func=lambda c: c.id)
    events = diff_layer.diff(previous_courses, latest_courses)

    added_courses = [e.after for e in events if e.type == ChangeEventType.ADDED]

    # filter only for relevant districts
    courses_of_interest = [course for course in added_courses if course.district in ['BVV', 'BVV/Sch']]

    if len(courses_of_interest) == 0:
        return False  # no new courses

    mailer = Mailer(smtp_settings)
    for course in courses_of_interest:
        mail_constructor = MailConstructor(
            from_mail=("SR Management", smtp_settings.username),
            to_mail=(None, smtp_settings.username),
            subject=f"Neuer SR Kurs: {course.label}"
        )
        mail_constructor.plain_text = str(course)
        mailer.send_mail(mail_constructor.get_mail())


def main(program_path):
    logging.basicConfig(filename=os.path.join(program_path, "recent.log"), encoding="utf-8", level=logging.DEBUG)
    db_path = "ref_management_db.sql"

    with open("config.json", "r") as f:
        config = json.load(f)

    bvv_credentials = config['bvv_credentials']
    bvv_username = bvv_credentials['username']
    bvv_password = bvv_credentials['password']
    scrape_and_save_data((bvv_username, bvv_password), db_path)

    mail_credentials = config['mail_credentials']
    smtp_settings = SMTPSettings(
        host=mail_credentials['smtp_host'],
        port=mail_credentials['smtp_port'],
        username=mail_credentials['smtp_username'],
        password=mail_credentials['smtp_password']
    )
    send_new_course_notification(db_path, smtp_settings)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # print("Usage: python script.py <config_path>")
        sys_cwd = os.getcwd()
    else:
        sys_cwd = sys.argv[1]

    main(sys_cwd)
