from dataclasses import fields

from manager.bvv_tools import BVVClient, normalize_user_token
from manager.models import PersonIdentity


def get_token_from_person(person: PersonIdentity) -> str:
    if not isinstance(person, PersonIdentity):
        raise TypeError("person must be of type PersonIdentity")
    for f in fields(person):
        if getattr(person, f.name) is None:
            raise ValueError(f"person must be fully specified, but {f.name} is None")
    return BVVClient.create_user_token(
        last_name=person.last_name,
        first_name=person.first_name,
        birth_date=person.birth_date,  # type: ignore
        user_id=person.id  # type: ignore
    )

def get_person_from_token(user_token: str) -> PersonIdentity:
    return normalize_user_token(user_token)
