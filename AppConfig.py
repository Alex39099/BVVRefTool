#  Copyright (c) 2026. Alexander Schmid
import json
from dataclasses import dataclass
from pathlib import Path


class FromDictMixin:
    @classmethod
    def from_dict(cls, data: dict) -> "FromDictMixin":
        return cls(**data)  # type: ignore[call-arg]


@dataclass(frozen=True)
class GeneralSettings(FromDictMixin):
    districts: list[str]


@dataclass(frozen=True)
class SMTPSettings(FromDictMixin):
    host: str
    port: int
    username: str
    password: str


@dataclass(frozen=True)
class BVVSettings(FromDictMixin):
    username: str
    password: str
    club_id: str


@dataclass(frozen=True)
class GoogleSheetsSettings(FromDictMixin):
    oauth_client_file_path: str
    token_file_path: str


@dataclass(frozen=True)
class SubscriptionSettings:
    from_mail: tuple[str, str]
    spreadsheet_id: str
    unsubscribe_endpoint: str
    unsubscribe_token_secret: str
    course_base_url: str
    i18n: dict[str, str]
    new_course_mail_template: str

    @classmethod
    def from_dict(cls, data: dict, template_dir: Path) -> "SubscriptionSettings":
        return cls(
            from_mail=(data['from_name'], data['from_mail']),
            spreadsheet_id=data['spreadsheet_id'],
            unsubscribe_endpoint=data['unsubscribe_endpoint'],
            unsubscribe_token_secret=data['unsubscribe_token_secret'],
            course_base_url=data['course_base_url'],
            i18n=data.get('i18n', {}),
            new_course_mail_template=(template_dir / data['templates']['new_course_mail']).read_text(encoding='utf-8')
        )


@dataclass(frozen=True)
class AppConfig:
    general: GeneralSettings
    bvv: BVVSettings
    smtp: SMTPSettings
    google_sheets: GoogleSheetsSettings
    subscription: SubscriptionSettings

    @classmethod
    def from_file(cls, config_path: str | Path) -> "AppConfig":
        config_path = Path(config_path)
        config = json.loads(config_path.read_text(encoding='utf-8'))

        return cls(
            general=GeneralSettings.from_dict(config['general']),
            bvv=BVVSettings.from_dict(config['bvv_credentials']),
            smtp=SMTPSettings.from_dict(config['smtp_credentials']),
            google_sheets=GoogleSheetsSettings.from_dict(config['google_sheets']),
            subscription=SubscriptionSettings.from_dict(config['subscription_srv'], template_dir=config_path.parent)
        )

