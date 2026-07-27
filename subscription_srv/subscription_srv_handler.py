import base64
import hashlib
import hmac
import logging
from dataclasses import dataclass

from google.auth.credentials import Credentials

from config.AppConfig import AppConfig
from helper.GoogleService import read_spreadsheet_data
from helper.Mailing import MailConstructor, Mailer
from manager.Data import (
    Course,
    CourseType,
    GrantableLicense,
    GrantableLicenseCategory,
    GrantableLicenseType,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Recipient:
    name: str
    mail: str
    active: bool


@dataclass
class SubscriptionService:
    mailer: Mailer
    from_mail: tuple[str, str]
    spreadsheet_id: str
    gc_credentials: Credentials
    unsubscribe_endpoint: str
    unsubscribe_token_secret: str
    course_base_url: str
    i18n: dict[str, str]
    new_course_mail_template: str

    @classmethod
    def from_config(cls, config: AppConfig, gc_credentials: Credentials) -> "SubscriptionService":
        return cls(
            mailer=Mailer(config.smtp, debug=config.debug),
            from_mail=config.subscription.from_mail,
            spreadsheet_id=config.subscription.spreadsheet_id,
            gc_credentials=gc_credentials,
            unsubscribe_endpoint=config.subscription.unsubscribe_endpoint,
            unsubscribe_token_secret=config.subscription.unsubscribe_token_secret,
            course_base_url=config.subscription.course_base_url,
            i18n=config.subscription.i18n,
            new_course_mail_template=config.subscription.new_course_mail_template
        )

    def get_recipients_from_gc(self) -> list[Recipient]:
        spreadsheet_data = read_spreadsheet_data(
            spreadsheet_id=self.spreadsheet_id,
            range_name="mailing_backend!A2:D",  # exclude header
            credentials=self.gc_credentials
        )

        return [
            Recipient(
                name=str(row[0]),
                mail=str(row[1]),
                active=bool(row[2])
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

    def send_new_course_notifications(self, added_courses: list[Course]) -> None:
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
            course_url = f"{self.course_base_url}{course.id}"
            course_html = course.to_html(skip_empty=True, course_url=course_url, labels=self.i18n)

            # send mail to each recipient individually
            for recipient in recipients:
                if not recipient.active:
                    logger.info(f"skipping inactive recipient {(recipient.name, recipient.mail)}")
                    continue
                
                try:
                    mail_constructor.html_text = self.build_new_course_mail_html(recipient.mail, course_html)

                    mail_constructor.to_mails = [(recipient.name, recipient.mail)]
                    self.mailer.send_mail(mail_constructor.get_mail())
                except Exception as e:
                    logger.error(f"failed to send mail to {(recipient.name, recipient.mail)} because {e}")


def make_hmac_token(secret, email) -> str:
    sig = hmac.new(secret.encode(), email.encode(), hashlib.sha256).hexdigest()
    token = base64.urlsafe_b64encode(f"{email}|{sig}".encode()).decode()
    return token
