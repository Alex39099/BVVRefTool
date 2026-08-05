from __future__ import annotations

import json
from collections.abc import Iterator, MutableMapping
from typing import Any


class SheetDeveloperMetadata(MutableMapping):
    METADATA_KEY = "SheetDeveloperMetadata"
    
    _sheet_id: int
    _metadata_id: int | None
    _data: dict[str, str]
    _snapshot: dict[str, str]
    
    def __init__(self, sheet_id: int, id: int | None = None, data: dict[str, str] | None = None) -> None:
        """ Constructs a SheetDeveloperMetadata.

        Args:
            sheet_id (int): the id of the sheet this metadata belongs to.
            id (int | None, optional): Internal. Do not set.
            data (dict[str, str] | None, optional): data of the metadata. Defaults to an empty dict.
        """
        self._sheet_id = sheet_id
        self._metadata_id = id
        self._data = dict(data or {})
        self._snapshot = dict(self._data or {})
        
    @classmethod
    def from_json(cls, jsonobject: dict[str, Any]) -> SheetDeveloperMetadata:
        return cls(
            sheet_id=jsonobject['location']['sheetId'],
            id=jsonobject.get('metadataId'),
            data=json.loads(jsonobject['metadataValue'])
        )
        
    def _copy_to(self, sheet_id: int) -> SheetDeveloperMetadata:
        """ Internal. Use Spreadsheet methods instead. """
        new_metadata = SheetDeveloperMetadata(sheet_id=sheet_id, data=self._data)
        new_metadata._snapshot = {}
        return new_metadata
    
    def to_json(self):
        data = {
            "metadataKey": self.METADATA_KEY,
            "metadataValue": json.dumps(self._data),
            "location": {"sheetId": self._sheet_id}
        }
        if self._metadata_id is not None:
            data['metadataId'] = self._metadata_id
        return data
    
    @property
    def request_json(self):
        keys_to_remove = {'location'}
        writing_json = {k: v for k, v in self.to_json() if k not in keys_to_remove}
        return writing_json
    
    @property
    def is_dirty(self):
        return self._data != self._snapshot
    
    def mark_clean(self):
        self._snapshot = dict(self._data)
        
    @property
    def dirty_field_mask(self):
        return "metadataValue"
        
    @property
    def sheet_id(self) -> int:
        return self._sheet_id
    
    @property
    def id(self) -> int | None:
        return self._metadata_id
    
    @id.setter
    def id(self, value: int) -> None:
        if self._metadata_id is not None and self._metadata_id != value:
            raise ValueError(f"metadata_id already set to {self._metadata_id}")
        if not isinstance(value, int):
            raise TypeError("value must be of type int")
        self._metadata_id = value
    
    def __getitem__(self, key):
        return self._data.__getitem__(key)
    
    def __setitem__(self, key: str, value: str) -> None:
        if not isinstance(key, str):
            raise TypeError("key must be of type str")
        if not isinstance(value, str):
            raise TypeError("value must be of type str")
        return self._data.__setitem__(key, value)
        
    def __delitem__(self, key):
        return self._data.__delitem__(key)
        
    def __contains__(self, key: object) -> bool:
        return self._data.__contains__(key)
    
    def __iter__(self) -> Iterator:
        return self._data.__iter__()
    
    def __len__(self) -> int:
        return self._data.__len__()
