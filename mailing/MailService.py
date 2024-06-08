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
from datetime import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from smtplib import SMTP

from data.Config import Config
from mailing.MessageCreator import ManagementReport


class Mailer:
    __instance = None

    @staticmethod
    def instance(config: Config = None):
        """
        Get an instance of Mailer. First call must have config as parameter
        :param config: the config file
        :return: unique instance of Mailer.
        """
        if Mailer.__instance is None:
            if config is None:
                raise ValueError("first call of get_instance must have config.")
            Mailer.__instance = Mailer(config)
        return Mailer.__instance

    def __init__(self, config: Config):
        credentials = config.get(["mail_settings"])
        self.mails_only_to_management = credentials['mails_only_to_management']

        self.smtp_server = credentials['smtp_server']
        self.smtp_port = credentials['smtp_port']
        self.smtp_username = credentials['smtp_username']
        self.smtp_password = credentials['smtp_password']

        self.management_contact_name = config.get(["mail_settings", "contact_info", "name"])
        self.management_contact_mail = config.get(["mail_settings", "contact_info", "mail"])
        self.management_contact_phone = config.get(["mail_settings", "contact_info", "phone"])

        config_path_mail_header = ["mail_settings", "mail_headers"]
        mail_template_keys = config.get(config_path_mail_header).keys()

        self.mail_templates = {}
        directory = os.path.join(os.getcwd(), config.get(["general", "main_folder_path"]))
        for key in mail_template_keys:
            self.mail_templates[key] = {
                "header": config.get(config_path_mail_header + [key]),
                "body": Path(os.path.join(directory, f"mail_{key}.txt")).read_text(encoding='utf-8')
            }

        self.management_report_mail = config.get(["mail_settings", "mail_notifications", "management", "mail_to"])

    def _send_mail(self, mail_to, msg_str):
        logging.info(f"MailService: sending mail to {mail_to}")
        if self.mails_only_to_management and (mail_to != self.management_report_mail):
            mail_to = self.management_report_mail

        with SMTP(self.smtp_server, self.smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(self.smtp_username, self.smtp_password)
            smtp.sendmail(self.smtp_username, mail_to, msg_str)
        return

    def send_management_report(self, html_report):
        if html_report is None:
            logging.info("Management report has no content. Skipping mailing...")
            return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Management Report {datetime.today().strftime('%d.%m.%Y, %H:%M')}"
        msg["From"] = self.management_report_mail
        msg["To"] = self.management_report_mail

        msg.attach(MIMEText(html_report, "plain"))  # change to some plain text version...
        msg.attach(MIMEText(html_report, "html"))

        self._send_mail(self.management_report_mail, msg.as_string())
        return

    def _send_mail_from_template(self, mail_template_key, data):
        additional_placeholders = {
            "management_contact_name": self.management_contact_name,
            "management_contact_mail": self.management_contact_mail,
            "management_contact_phone": self.management_contact_phone
        }

        for dict in data.to_dict('records'):
            placeholders = {**dict, **additional_placeholders}
            header = self.mail_templates[mail_template_key]["header"].format(**placeholders)
            body = self.mail_templates[mail_template_key]["body"].format(**placeholders)

            msg = MIMEText(body, 'plain', 'utf-8')
            msg['Subject'] = Header(header)
            msg["From"] = self.management_report_mail
            msg["To"] = dict["person_mail"]

            self._send_mail(msg["To"], msg.as_string())

    def send_course_success(self, big_registrations_df, course_type):
        self._send_mail_from_template(f"course_success_{course_type}", big_registrations_df)

    def send_course_fail(self, big_registrations_df):
        self._send_mail_from_template("course_failed", big_registrations_df)

    def send_course_cancellation(self, big_registrations_df):
        self._send_mail_from_template("course_cancelled", big_registrations_df)

    def send_course_missed(self, big_registrations_df):
        self._send_mail_from_template("course_missed", big_registrations_df)

    def send_course_confirmation_request(self, big_registrations_df):
        self._send_mail_from_template("course_ask_confirmation", big_registrations_df)

    def send_course_confirmed(self, big_registrations_df, refresher_online=False):
        if refresher_online:
            self._send_mail_from_template("course_confirmed_refresher_online", big_registrations_df)
            return
        self._send_mail_from_template("course_confirmed", big_registrations_df)

    def send_course_reminder(self, big_registrations_df, course_type, course_online=False):
        if course_online:
            course_type = f"{course_type}_online"
        self._send_mail_from_template(f"course_reminder_{course_type}", big_registrations_df)

    def send_ref_request(self, recipients_df):
        self._send_mail_from_template("ref_request", recipients_df)
