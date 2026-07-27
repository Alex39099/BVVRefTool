#  Copyright (c) 2026. Alexander Schmid
import dataclasses
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Self


class FromDictMixin:
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        valid_keys = {f.name for f in dataclasses.fields(cls)}  # type: ignore[arg-type]
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

@dataclass(frozen=True)
class GeneralSettings:
    db_path: str
    districts: list[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any], config_dir: Path) -> "GeneralSettings":
        return cls(
            db_path=config_dir / data['db_path'],
            districts=data['districts']
        )

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
class GoogleServiceAccountAuthorizationSettings:
    service_account_key_file_path: Path
    impersonate_user: str | None = None
    
    @classmethod
    def from_dict(cls, data: dict[str, Any], config_dir: Path) -> "GoogleServiceAccountAuthorizationSettings":
        return cls(
            service_account_key_file_path=config_dir / data['service_account_key_file_path'],
            impersonate_user=data.get('impersonate_user')
        )


@dataclass(frozen=True)
class GoogleOAuthAuthorizationSettings:
    oauth_client_file_path: Path
    oauth_token_file_path: Path
    
    @classmethod
    def from_dict(cls, data: dict, config_dir: Path) -> "GoogleOAuthAuthorizationSettings":
        return cls(
            oauth_client_file_path=config_dir / data['oauth_client_file_path'],
            oauth_token_file_path=config_dir / data['oauth_token_file_path']
        )


class AuthorizationType(StrEnum):
    SERVICE_ACCOUNT = "service_account"
    OAUTH = "oauth"


@dataclass(frozen=True)
class GoogleSettings:
    authorization_type: AuthorizationType
    service_account_authorization: GoogleServiceAccountAuthorizationSettings | None = None
    oauth_authorization: GoogleOAuthAuthorizationSettings | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], config_dir: Path) -> "GoogleSettings":
        authorization_type = AuthorizationType(data['authorization_type'])
        service_account_settings = data.get('service_account_authorization')
        oauth_settings = data.get('oauth_authorization')
        
        if authorization_type == AuthorizationType.SERVICE_ACCOUNT and not service_account_settings:
            raise ValueError("service_account_authorization settings are required for service account authorization")
        if authorization_type == AuthorizationType.OAUTH and not oauth_settings:
            raise ValueError("oauth_authorization settings are required for oauth authorization")
        
        return cls(
            authorization_type=AuthorizationType(data['authorization_type']),
            service_account_authorization=GoogleServiceAccountAuthorizationSettings.from_dict(service_account_settings, config_dir) if service_account_settings else None,
            oauth_authorization=GoogleOAuthAuthorizationSettings.from_dict(oauth_settings, config_dir) if oauth_settings else None
        )


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
    def from_dict(cls, data: dict[str, Any], template_dir: Path) -> "SubscriptionSettings":
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
    google: GoogleSettings
    subscription: SubscriptionSettings
    debug: bool = False

    @classmethod
    def from_file(cls, config_path: str | Path) -> "AppConfig":
        config_path = Path(config_path).resolve()
        config_dir = config_path.parent
        config = json.loads(config_path.read_text(encoding='utf-8'))

        return cls(
            debug=config.get('debug', False),
            general=GeneralSettings.from_dict(config['general'], config_dir=config_dir),
            bvv=BVVSettings.from_dict(config['bvv_credentials']),
            smtp=SMTPSettings.from_dict(config['smtp_credentials']),
            google=GoogleSettings.from_dict(config['google_credentials'], config_dir=config_dir),
            subscription=SubscriptionSettings.from_dict(config['subscription_srv'], template_dir=config_dir)
        )
