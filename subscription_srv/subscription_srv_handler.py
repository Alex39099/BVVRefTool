import base64
import hashlib
import hmac
import logging
from dataclasses import dataclass
from pathlib import Path

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
    unsubscribe_endpoint: str
    unsubscribe_token_secret: str

    new_course_mail_template = Path("new_course_mail_template.html").read_text(encoding='utf-8')

    def get_recipients_from_gc(self) -> list[Recipient]:
        spreadsheet_data = read_spreadsheet_data(
            spreadsheet_id=self.spreadsheet_id,
            range_name="mailing_backend!A2:D",  # exclude header
            credentials=self.gc_credentials
        )

        return [
            Recipient(
                name=row[0],
                mail=row[1],
                active=row[2],
                token=row[3] if len(row) > 3 else None
            )
            for row in spreadsheet_data
        ]

    def make_unsubscribe_token(self, email):
        return make_hmac_token(self.unsubscribe_token_secret, email)

    def build_unsubscribe_html_footer(self, email: str) -> str:
        token = self.make_unsubscribe_token(email)
        link = f"{self.unsubscribe_endpoint}?token={token}"

        return f"""
        <hr>
        <p style="font-size:12px;color:#666;">
            <a href="{link}">Unsubscribe</a>
        </p>
        """

    def build_new_course_mail_html(self, email: str, course_html: str) -> str:
        unsubscribe_footer = self.build_unsubscribe_html_footer(email)
        return self.new_course_mail_template.format(
            course_html=course_html,
            unsubscribe_footer=unsubscribe_footer
        )

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
            any(lic in grantable_licenses_of_interest for lic in course.grantable_licenses) and
            course.type in course_types_of_interest
        )]

        logger.info(f"Sending new course notifications for courses: {[(course.id, course.label) for course in courses_of_interest]}")

        for course in courses_of_interest:
            mail_constructor = MailConstructor(
                from_mail=self.from_mail,
                subject=f"Neuer SR Lehrgang: {course.label} ({course.city})"
            )

            course_html = course.to_html(skip_empty=True)

            # send mail to each recipient individually
            for recipient in recipients:
                try:
                    mail_constructor.html_text = self.build_new_course_mail_html(recipient.mail, course_html)

                    mail_constructor.to_mails = [(recipient.name, recipient.mail)]
                    self.mailer.send_mail(mail_constructor.get_mail())
                except Exception as e:
                    logger.error(f"failed to send mail to {(recipient.name, recipient.mail)} because {e}")


def make_hmac_token(secret, email):
    sig = hmac.new(secret.encode(), email.encode(), hashlib.sha256).hexdigest()
    token = base64.urlsafe_b64encode(f"{email}|{sig}".encode()).decode()
    return token
