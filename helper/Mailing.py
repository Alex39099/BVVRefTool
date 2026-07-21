#  Copyright (c) 2026. Alexander Schmid
import copy
import logging
import mimetypes
import smtplib
from dataclasses import dataclass, field, InitVar
from email.message import EmailMessage
from email.utils import getaddresses, formataddr

import html2text

from config.AppConfig import SMTPSettings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass(frozen=True)
class Attachment:
    binary: bytes
    filename: str
    type: str | None = None

    def __post_init__(self):
        if self.type is None:
            guessed, _ = mimetypes.guess_type(self.filename)
            object.__setattr__(self, "type", guessed)

        if self.type is None:
            raise ValueError("could not determine MIME type")

        split = self.type.split("/", 1)
        if len(split) != 2:
            raise ValueError(f"type {self.type} is not valid, expected 'maintype/subtype'")

    @property
    def maintype(self) -> str | None:
        return self.type.split('/', 1)[0] if self.type else None

    @property
    def subtype(self) -> str | None:
        return self.type.split('/', 1)[1] if self.type else None

@dataclass
class MailConstructor:
    from_mail: tuple[str | None, str]
    subject: str

    to_mail: InitVar[tuple[str | None, str] | None] = None
    to_mails: list[tuple[str | None, str]] = field(default_factory=list)
    cc_mails: list[tuple[str | None, str]] = field(default_factory=list)
    bcc_mails: list[tuple[str | None, str]] = field(default_factory=list)

    plain_text: str | None = None
    html_text: str | None = None
    attachments: list[Attachment] = field(default_factory=list)

    def __post_init__(self, to_mail: tuple[str | None, str] | None = None):
        if to_mail is not None and to_mail not in self.to_mails:
            self.to_mails.append(to_mail)

    @staticmethod
    def _get_addresses(addr_list: list[tuple[str | None, str]]):
        return ', '.join(formataddr(addr) for addr in addr_list)

    def get_mail(self) -> EmailMessage:
        if len(self.to_mails) == 0:
            raise ValueError("no main recipients set")

        if not (self.plain_text or self.html_text):
            raise ValueError("neither plain_text or html_text is set")
        plain_text = self.plain_text if self.plain_text else html2text.html2text(self.html_text) # type: ignore

        msg = EmailMessage()
        msg.set_content(plain_text)
        if self.html_text:
            msg.add_alternative(self.html_text, subtype='html')

        msg['From'] = formataddr(self.from_mail)
        msg['Subject'] = self.subject
        msg['To'] = self._get_addresses(self.to_mails)
        if self.cc_mails:
            msg['Cc'] = self._get_addresses(self.cc_mails)
        if self.bcc_mails:
            msg['Bcc'] = self._get_addresses(self.bcc_mails)

        for att in self.attachments:
            msg.add_attachment(att.binary, maintype=att.maintype, subtype=att.subtype, filename=att.filename)

        return msg


@dataclass(frozen=True)
class Mailer:
    smtp_settings: SMTPSettings | None  # use None for debugging
    debug: bool = False

    def send_mail(self, msg: EmailMessage | MailConstructor, send_separately: bool = False):
        if isinstance(msg, MailConstructor):
            msg = msg.get_mail()

        main_recipients = [(name, addr) for name, addr in getaddresses(msg.get_all("To", []))]
        
        if self.debug:
            logger.info(f"[DEBUG] would send mail to {main_recipients} (excluding cc, bcc): {msg}")
            return

        if self.smtp_settings is None:
            raise ValueError("smtp_settings must be provided when not in debug")

        with smtplib.SMTP(self.smtp_settings.host, self.smtp_settings.port) as srv:
            srv.ehlo()  # say hello
            srv.starttls()  # update TCP to TLS connection
            srv.ehlo()  # re-identify as TLS connection
            srv.login(self.smtp_settings.username, self.smtp_settings.password)

            if send_separately:
                for main_recipient in main_recipients:
                    # copy msg and adjust 'To' argument
                    mail = copy.deepcopy(msg)
                    mail['To'] = formataddr(main_recipient)

                    # send mail separately
                    srv.send_message(mail)
            else:
                # send mail
                srv.send_message(msg)

        logger.info(f"sent mail to {main_recipients} (excluding cc, bcc): {msg}")
