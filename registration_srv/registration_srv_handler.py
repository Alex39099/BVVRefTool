from dataclasses import dataclass
from datetime import date
import logging
from typing import Any, Callable

from google.oauth2.credentials import Credentials

from config.AppConfig import AppConfig
from manager.BVVTools import BVVClient, BVVSession
from manager.Data import PersonIdentity, Referee, Registration

logger = logging.getLogger(__name__)


@dataclass
class RegistrationCommand:
    action: Callable[[BVVSession], Any]
    description: str


class RegistrationQueue:
    def __init__(self, client: BVVClient):
        self._client = client
        self._queue: list[RegistrationCommand] = []
        
    def enqueue_register(self, user_id: str, course_id: str, course_type_raw: str) -> None:
        self._queue.append(RegistrationCommand(
            action=lambda session: self._client.register_person_to_course(session, user_id, course_id, course_type_raw),
            description=f"Register user {user_id} to course {course_id}"
        ))
        
    def enqueue_cancel(self, registration_id: str):
        self._queue.append(RegistrationCommand(
            action=lambda session: self._client.cancel_course_registration(session, registration_id),
            description=f"Cancel registration {registration_id}"
        ))
        
    def enqueue_change(self, registration_id: str, last_name: str, first_name: str, birth_date: date | None, user_id: str | None):
        user_token = None
        if birth_date is not None and user_id:
            logger.debug("creating user token locally")
            user_token = self._client.create_user_token(user_id=user_id, last_name=last_name, first_name=first_name, birth_date=birth_date)
            
        def change_action(session: BVVSession):
            nonlocal user_token
            if not user_token:
                user_tokens = self._client.find_user(session, f"{last_name} {first_name}")
                if len(user_tokens) == 1:
                    logger.debug("received user token from BVV site")
                    user_token = user_tokens[0]['value']
                else:
                    raise ValueError(f"Could not find unique user token for {last_name}, {first_name}. Found: {user_tokens}")
            
            return self._client.change_course_registration(session, registration_id, user_token)
        
        self._queue.append(RegistrationCommand(
            action=change_action,
            description=f"Change registration {registration_id} to user {last_name}, {first_name}"
        ))

    def execute(self) -> list[tuple[RegistrationCommand, Any | Exception]]:
        results = []
        with self._client.get_session() as session:
            for command in self._queue:
                try:
                    result = command.action(session)
                    results.append((command, result))
                except Exception as e:
                    logger.error(f"Failed command: {command.description} because {e}")
                    results.append((command, e))
        self._queue.clear()
        return results


@dataclass
class RegistrationService:
    registration_queue: RegistrationQueue
    bvv_members: list[PersonIdentity]
    bvv_referees: list[Referee]
    bvv_registrations: list[Registration]
    
    @classmethod
    def from_config(cls, config: AppConfig, gc_credentials: Credentials) -> "RegistrationService":
        raise NotImplementedError("RegistrationService.from_config is not implemented yet")
        # return cls(
        #     registration_queue=RegistrationQueue(client=BVVClient.from_config(config=config))
            
        # )
    
    
    