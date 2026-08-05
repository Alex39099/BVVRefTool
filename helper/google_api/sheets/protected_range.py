from __future__ import annotations

import json
from typing import Any

from helper.google_api.sheets._tracked_model import TrackedModel
from helper.google_api.sheets.grid_range import GridRange


class ProtectedRange(TrackedModel):
    
    _jsonobject: dict[str, Any]
    
    def __init__(self, jsonobject: dict[str, Any] | None = None, range: GridRange | dict[str, int] | None = None, is_dirty: bool = True):
        """ Constructs a new ProtectedRange

        Args:
            jsonobject (dict[str, Any] | None, optional): the cloud jsonobject. This takes precedence over range param. Defaults to None. 
            range (GridRange | dict[str, int] | None, optional): gridRange of this protected range. Defaults to None.
            is_dirty (bool, optional): True if this protectedRange is not yet synced. Defaults to True.

        Raises:
            ValueError: if no range was specfied, either via jsonobject or range.
        """
        super().__init__(is_dirty)
        self._jsonobject: dict[str, Any] = {}
        if jsonobject is not None:
            self._jsonobject: dict[str, Any] = json.loads(json.dumps(jsonobject))
        elif range is not None:
            if isinstance(range, dict):
                range = GridRange.from_json(range)
            self._jsonobject.setdefault("range", range.to_json())
        else:
            raise ValueError("either jsonobject or range must be set")
            
        if not self._jsonobject.get("range"):
            raise ValueError(f"missing range: {self._jsonobject}")
        
        self._jsonobject.setdefault("description", "")
        self._jsonobject.setdefault("editors", {"groups": [], "users": []})
        self._jsonobject["editors"].setdefault("groups", [])
        self._jsonobject["editors"].setdefault("users", [])
        
    @classmethod
    def from_json(cls, jsonobject: dict[str, Any], is_dirty: bool = False) -> ProtectedRange:
        """ Construct a protectedRange from the json representation

        Args:
            jsonobject (dict[str, Any]): json representation of the protectedRange
            is_dirty (bool, optional): False if this protectedRange is synced with cloud. Defaults to False.

        Returns:
            ProtectedRange: _description_
        """
        return cls(jsonobject, is_dirty = is_dirty)
    
    def to_json(self) -> dict[str, Any]:
        """ Returns the full json representation of this protectedRange.

        Returns:
            dict[str, Any]: json object of this protected Range
        """
        return json.loads(json.dumps(self._jsonobject))
        
    def _copy_to(self, sheet_id: int) -> ProtectedRange:
        json_copy = json.loads(json.dumps(self._jsonobject))
        json_copy['range']['sheetId'] = sheet_id
        if 'protectedRangeId' in json_copy:
            json_copy.pop('protectedRangeId')
        return ProtectedRange(jsonobject=json_copy, is_dirty=True)
    
    @property
    def id(self) -> int | None:
        return self._jsonobject.get('protectedRangeId')
    
    @id.setter
    def id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("id must be of type int")
        if self._jsonobject.get('protectedRangeId') is not None:
            raise ValueError("id cannot be changed once set")
        self._jsonobject['protectedRangeId'] = id
    
    @property
    def description(self) -> str:
        return self._jsonobject['description']
    
    @description.setter
    def description(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("description must be a string")
        self._jsonobject['description'] = value
        self._mark_dirty('description')
            
    @property
    def warningOnly(self) -> bool:
        return self._jsonobject['warningOnly']
    
    @warningOnly.setter
    def warningOnly(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("warningOnly must be of type bool")
        self._jsonobject['warningOnly'] = value
        self._mark_dirty('warningOnly')
    
    @property
    def groups(self) -> set[str]:
        return set(self._jsonobject.setdefault("editors", {}).setdefault("groups", []))
    
    @groups.setter
    def groups(self, value: set[str]) -> None:
        if not isinstance(value, set):
            raise TypeError("groups must be a set of non-empty strings")
        if not all(isinstance(group, str) and group for group in value):
            raise ValueError("groups must be a set of non-empty strings")
        self._jsonobject.setdefault("editors", {})["groups"] = list(value)
        self._mark_dirty('editors')
        
    @property
    def users(self) -> set[str]:
        return set(self._jsonobject.setdefault("editors", {}).setdefault("users", []))
        
    @users.setter
    def users(self, value: set[str]) -> None:
        if not isinstance(value, set):
            raise TypeError("users must be a set of non-empty strings")
        if not all(isinstance(item, str) and item for item in value):
            raise ValueError("users must be a set of non-empty strings")
        self._jsonobject.setdefault("editors", {})["users"] = list(value)
        self._mark_dirty('editors')
        
    @property
    def range(self) -> GridRange:
        return GridRange.from_json(self._jsonobject['range'])
    
    @range.setter
    def range(self, value) -> None:
        if isinstance(value, dict):
            value = GridRange.from_json(value)
        if not isinstance(value, GridRange):
            raise TypeError("value must be a GridRange or dict/json representing GridRange")
        self._jsonobject['range'] = value.to_json()
        self._mark_dirty('range')
        
    @property
    def request_json(self) -> dict[str, Any]:
        keys_to_remove = {'namedRangeId', 'tableId'}
        writing_json = {k: v for k, v in self._jsonobject.items() if k not in keys_to_remove}
        return writing_json
