import logging
from dataclasses import dataclass

from google.oauth2.credentials import Credentials

from helper.GoogleSheets import read_spreadsheet_data
from helper.Mailing import MailConstructor, Mailer
from manager.Data import Course, GrantableLicense, GrantableLicenseType, GrantableLicenseCategory, CourseType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Recipient:
    name: str
    mail: str
    active: bool
    token: str | None = None


@dataclass
class SubscriptionService:
    mailer: Mailer
    from_mail: tuple[str, str]
    spreadsheet_id: str
    gc_credentials: Credentials

    def get_recipients_from_gc(self) -> list[Recipient]:
        spreadsheet_data = read_spreadsheet_data(
            spreadsheet_id=self.spreadsheet_id,
            range_name="A2:D", # exclude header
            credentials=self.gc_credentials
        )

        return [
            Recipient(
                name=row[0],
                mail=row[1],
                active=row[2],
                token=row.get(3)
            )
            for row in spreadsheet_data
        ]

    def send_new_course_notifications(self, added_courses: list[Course]):
        if not added_courses:
            logger.info("No courses added")
            return

        recipients = self.get_recipients_from_gc()

        if not recipients:
            logger.info("No recipients of subscription_srv")
            return

        # filter courses
        grantable_licenses_of_interest = {
            GrantableLicense(type=GrantableLicenseType.D, category=GrantableLicenseCategory.HALLE),
            GrantableLicense(type=GrantableLicenseType.CT, category=GrantableLicenseCategory.HALLE),
            GrantableLicense(type=GrantableLicenseType.CP, category=GrantableLicenseCategory.HALLE),
            GrantableLicense(type=GrantableLicenseType.C, category=GrantableLicenseCategory.HALLE)
        }

        course_types_of_interest = {
            CourseType.AUSBILDUNG
        }
        courses_of_interest = [course for course in added_courses if (
            course.district in ['BVV', 'BVV/Sch'] and
            any(lic in grantable_licenses_of_interest for lic in course.grantable_licenses) and
            course.type in course_types_of_interest
        )]

        logger.info(f"Sending new course notifications for courses: {[(course.id, course.label) for course in courses_of_interest]}")

        for course in added_courses:
            mail_constructor = MailConstructor(
                from_mail=self.from_mail,
                subject=f"Neuer SR Lehrgang: {course.label} ({course.city})"
            )
            mail_constructor.plain_text = str(course)
            mail_constructor.html_text = course.to_html()

            # send mail to each recipient individually
            for recipient in recipients:
                mail_constructor.to_mails = [(recipient.name, recipient.mail)]
                self.mailer.send_mail(mail_constructor.get_mail())
