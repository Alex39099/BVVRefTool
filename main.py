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

from functools import cache
import logging
import os
import sys
from datetime import datetime, timezone

from AppConfig import AppConfig, GoogleSettings
from helper import GoogleService
from helper.Mailing import MailConstructor, Mailer
from manager.BVVTools import BVVClient, parse_courses_from_html, normalize_course
from manager.Data import Course
from manager.DiffLayer import DiffLayer, ChangeEventType
from manager.Storage import SnapshotRepository, SnapshotSource, ScraperRunningStatus
from subscription_srv.subscription_srv_handler import SubscriptionService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def scrape_and_save_data(config: AppConfig) -> tuple[int, datetime]:
    scraper = BVVClient.from_config(config)
    snapshot_rep = SnapshotRepository(config.general.db_path)

    run_id, collected_at = snapshot_rep.create_run()
    scraped_data = {}

    # scrape data at once so we only have one login
    try:
        with scraper.get_session() as session:
            scraped_data[SnapshotSource.BVV_COURSES] = scraper.scrape_courses(session)
            scraped_data[SnapshotSource.BVV_REGISTRATIONS] = scraper.scrape_registrations(session)
            scraped_data[SnapshotSource.BVV_LICENSES_EXCEL] = scraper.scrape_licenses_excel(session)
            scraped_data[SnapshotSource.BVV_MEMBERS] = scraper.scrape_members(session)
        logger.info(f"all data was scraped for run_id {run_id}")
    except Exception as e:
        logger.error(f"Failed to scrape data for run_id {run_id} because {e}")
        snapshot_rep.update_run_status(run_id, ScraperRunningStatus.FAILED)
        logger.exception(e)
        raise e

    # save data in repo
    for k, v in scraped_data.items():
        snapshot_rep.save_snapshot(run_id, source=k, raw_data=v)
        logger.info(f"saved snapshot: run_id = {run_id}, {k}")
    snapshot_rep.update_run_status(run_id, ScraperRunningStatus.SUCCESS)
    logger.info(f"all data was saved for run_id = {run_id}. Status = {ScraperRunningStatus.SUCCESS}")
    return run_id, collected_at


def send_new_course_notification_management(config: AppConfig):
    snapshot_repo = SnapshotRepository(config.general.db_path)
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

    diff_layer = DiffLayer[Course](key_func=lambda c: c.id)
    events = diff_layer.diff(previous_courses, latest_courses)

    added_courses = [e.after for e in events if e.type == ChangeEventType.ADDED]
    # filter only for relevant districts
    courses_of_interest = [course for course in added_courses if course is not None and course.district in config.general.districts]

    # sending mail to management
    if len(courses_of_interest) == 0:
        return False  # no new courses

    mailer = Mailer(config.smtp)
    for course in courses_of_interest:
        mail_constructor = MailConstructor(
            from_mail=("SR Management", config.smtp.username),
            to_mail=(None, config.smtp.username),
            subject=f"Neuer SR Kurs: {course.label} ({course.city})"
        )
        course_url = f"{config.subscription.course_base_url}?lid={course.id}"
        labels = config.subscription.i18n
        mail_constructor.html_text = course.to_html(course_url=course_url, labels=labels)
        mailer.send_mail(mail_constructor.get_mail())


def subscription_srv(config: AppConfig):
    snapshot_repo = SnapshotRepository(config.general.db_path)
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

    diff_layer = DiffLayer[Course](key_func=lambda c: c.id)
    events = diff_layer.diff(previous_courses, latest_courses)

    added_courses = [e.after for e in events if e.type == ChangeEventType.ADDED]
    # filter only for relevant districts
    courses_of_interest = [course for course in added_courses if course is not None and course.district in config.general.districts]

    if len(courses_of_interest) == 0:
        logger.info("no courses of interest for subscription srv")
        return

    # Google credentials
    gc_credentials = get_google_credentials(config.google)

    # Subscription Service
    srv = SubscriptionService.from_config(config=config, gc_credentials=gc_credentials)
    srv.send_new_course_notifications(courses_of_interest)
    logger.info("subscription service finished")
    
    
@cache
def get_google_credentials(google_config: GoogleSettings) -> GoogleService.BaseCredentials:
    return GoogleService.authorize(google_config)


def main(program_path):
    log_dir = os.path.join(program_path, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(log_dir, f"{datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H-%MZ')}.log"), encoding="utf-8", level=logging.DEBUG)

    config_path = os.path.join(program_path, "config.json")
    config = AppConfig.from_file(config_path)

    # scrape new data
    run_id, collected_at = scrape_and_save_data(config)

    # send course notification to management
    send_new_course_notification_management(config)

    # subscription service
    try:
        subscription_srv(config=config)
    except Exception as e:
        logger.error("Something went wrong for subscription service")
        logger.exception(e)

        # send mail to management
        mailer = Mailer(config.smtp)
        mail_constructor = MailConstructor(
            from_mail=("SR Management", config.smtp.username),
            to_mail=(None, config.smtp.username),
            subject="Subscription Service Error"
        )
        mail_constructor.plain_text = f"Something went wrong for subscription service: {e}"
        mailer.send_mail(mail_constructor.get_mail())

    # only keep latest snapshots from current run_id
    snapshot_rep = SnapshotRepository(config.general.db_path)
    snapshot_rep.delete_runs_older_than(collected_at)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # print("Usage: python script.py <config_dir>")
        sys_cwd = os.getcwd()
    else:
        sys_cwd = sys.argv[1]

    main(sys_cwd)
