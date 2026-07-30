import json
import re
from dataclasses import dataclass
from typing import Any


class TrackedList(list):
    def __init__(self, iterable, on_change):
        super().__init__(iterable)
        self._on_change = on_change

    def append(self, item):
        super().append(item)
        self._on_change()

    def remove(self, item):
        super().remove(item)
        self._on_change()

    def __setitem__(self, index, value):
        super().__setitem__(index, value)
        self._on_change()

    def __delitem__(self, index):
        super().__delitem__(index)
        self._on_change()

    def extend(self, iterable):
        super().extend(iterable)
        self._on_change()

    def pop(self, index=-1): # type: ignore
        value = super().pop(index)
        self._on_change()
        return value

    def insert(self, index, item):
        super().insert(index, item)
        self._on_change()

    def clear(self):
        super().clear()
        self._on_change()

class TrackedModel:
    # TODO REVIEW, especially the "_" filter and TrackedList
    
    _changed_fields: set[str]
    
    def __init__(self):
        object.__setattr__(self, "_changed_fields", set())

    def _mark_field_dirty(self, name: str):
        self._changed_fields.add(name)

    def __setattr__(self, name, value):
        if isinstance(value, list):
            value = TrackedList(value, on_change=lambda: self._mark_field_dirty(name))
        self._mark_field_dirty(name)
        object.__setattr__(self, name, value)

    @property
    def is_dirty(self) -> bool:
        return bool(self._changed_fields)

    @property
    def changed_fields(self) -> frozenset[str]:
        return frozenset(self._changed_fields)

    def mark_clean(self):
        object.__setattr__(self, "_changed_fields", set())
    
    
@dataclass(frozen=True)
class GridRange:
    sheet_id: int
    start_row_idx: int | None = None
    end_row_idx: int | None = None # inclusive
    start_column_idx: int | None = None
    end_column_idx: int | None = None # inclusive
    
    def __post_init__(self):
        # validate correct bounds
        # Rows
        if self.start_row_idx is None:
            if self.end_row_idx is not None:
                raise ValueError("end_row_idx must be None when start_row_idx is None")
        else:
            if self.start_row_idx < 0:
                raise ValueError("start_row_idx must be >= 0")

            if self.end_row_idx is not None and self.end_row_idx < self.start_row_idx:
                raise ValueError("end_row_idx must be >= start_row_idx")

        # Columns
        if self.start_column_idx is None:
            if self.end_column_idx is not None:
                raise ValueError("end_column_idx must be None when start_column_idx is None")
        else:
            if self.start_column_idx < 0:
                raise ValueError("start_column_idx must be >= 0")

            if self.end_column_idx is not None and self.end_column_idx < self.start_column_idx:
                raise ValueError("end_column_idx must be >= start_column_idx")
            
    @classmethod
    def from_a1_notation(cls, sheet_id: int, range_name: str) -> "GridRange":
        # strip sheet_name if present
        if "!" in range_name:
            _, range_name = range_name.rsplit("!", 1)
        
        def col_to_idx(col: str) -> int:
            idx = 0
            for c in col.upper():
                idx = idx * 26 + (ord(c) - ord("A") + 1)
            return idx - 1

        def parse_ref(ref: str) -> tuple[int | None, int | None]:
            match = re.fullmatch(r"([A-Za-z]*)(\d*)", ref)
            if match is None:
                raise ValueError(f"Invalid A1 reference: {ref}")

            col, row = match.groups()

            return (
                col_to_idx(col) if col else None,
                int(row) - 1 if row else None,
            )

        parts = range_name.split(":", 1)

        if len(parts) == 1:
            start_col, start_row = parse_ref(parts[0])
            end_col, end_row = start_col, start_row
        else:
            start_col, start_row = parse_ref(parts[0])
            end_col, end_row = parse_ref(parts[1])

        return cls(
            sheet_id=sheet_id,
            start_row_idx=start_row,
            end_row_idx=end_row,
            start_column_idx=start_col,
            end_column_idx=end_col,
        )
    
    @classmethod
    def from_json(cls, data: dict[str, int]) -> "GridRange":
        end_row_idx = data['endRowIndex'] - 1 if 'endRowIndex' in data else None
        end_column_idx = data['endColumnIndex'] - 1 if 'endColumnIndex' in data else None
        return cls(
            sheet_id=data["sheetId"],
            start_row_idx=data.get('startRowIndex'),
            end_row_idx=end_row_idx,
            start_column_idx=data.get("startColumnIndex"),
            end_column_idx=end_column_idx,
        )
    
    def to_json(self) -> dict[str, int]:
        data = {"sheetId": self.sheet_id}
        if self.start_row_idx is not None:
            data['startRowIndex'] = self.start_row_idx
        if self.end_row_idx is not None:
            data['endRowIndex'] = self.end_row_idx + 1
        if self.start_column_idx is not None:
            data['startColumnIndex'] = self.start_column_idx
        if self.end_column_idx is not None:
            data['endColumnIndex'] = self.end_column_idx + 1
        
        return data

       
class ProtectedRange:
    
    _jsonobject: dict[str, Any]
    _is_dirty: bool
    
    def __init__(self, jsonobject: dict[str, Any] | None = None, range: GridRange | dict[str, int] | None = None, is_dirty: bool = True):
        self._jsonobject: dict[str, Any] = {}
        self._is_dirty = is_dirty
        if jsonobject is not None:
            self._jsonobject: dict[str, Any] = json.loads(json.dumps(self._jsonobject))
        elif range is not None:
            if isinstance(range, dict):
                range = GridRange.from_json(range)
            self._jsonobject.setdefault("range", range.to_json())
        else:
            raise ValueError("either jsonobject or range must be set")
            
        if not self._jsonobject.get("range"):
            raise ValueError("missing range")
        
        self._jsonobject.setdefault("description", "")
        self._jsonobject.setdefault("editors", {"groups": [], "users": []})
        self._jsonobject["editors"].setdefault("groups", [])
        self._jsonobject["editors"].setdefault("users", [])
        
    @classmethod
    def from_json(cls, jsonobject: dict[str, Any], is_dirty: bool = False) -> "ProtectedRange":
        return cls(jsonobject, is_dirty = is_dirty)
    
    def to_json(self):
        # return a copy
        return json.loads(json.dumps(self._jsonobject))
    
    def _mark_dirty(self) -> None:
        self._is_dirty = True
        
    def mark_clean(self) -> None:
        self._is_dirty = False
        
    def duplicate(self) -> "ProtectedRange":
        json_copy = json.loads(json.dumps(self._jsonobject))
        if 'protectedRangeId' in json_copy:
            json_copy.pop('protectedRangeId')
        return ProtectedRange(jsonobject=json_copy, is_dirty=True)
    
    @property
    def is_dirty(self) -> bool:
        return self._is_dirty
    
    @property
    def id(self) -> int | None:
        return self._jsonobject.get('protectedRangeId')
    
    @id.setter
    def id(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("id must be of type int")
        current_id = self._jsonobject.get('protectedRangeId')
        if current_id != id:
            raise ValueError("id cannot be changed once set")
        self._jsonobject['protectedRangeId'] = id
        self._mark_dirty()
    
    @property
    def description(self) -> str:
        return self._jsonobject['description']
    
    @description.setter
    def description(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("description must be a string")
        self._jsonobject['description'] = value
        self._mark_dirty()
            
    @property
    def warningOnly(self) -> bool:
        return self._jsonobject['warningOnly']
    
    @warningOnly.setter
    def warningOnly(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("warningOnly must be of type bool")
        self._jsonobject['warningOnly'] = value
        self._mark_dirty()
    
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
        self._mark_dirty()
        
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
        self._mark_dirty()
        
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
        self._mark_dirty()
        
    def request_json(self) -> dict[str, Any]:
        keys_to_remove = {'namedRangeId', 'tableId'}
        writing_json = {k: v for k, v in self._jsonobject.items() if k not in keys_to_remove}
        return writing_json


@dataclass(frozen=True)
class DropDownValidationRule:
    a1_notation_range: str
    input_message: str
    strict: bool
        
    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "DropDownValidationRule":
        return cls(
            a1_notation_range=data['condition']['values'][0]['userEnteredValue'],
            input_message=data['inputMessage'],
            strict=True
        )
    
    def to_json(self) -> dict[str, Any]:
        return {
            "condition": {
                "type": "ONE_OF_RANGE",
                "values": [{
                    "userEnteredValue": self.a1_notation_range
                }]
            },
            "inputMessage": self.input_message,
            "strict": self.strict,
            "showCustomUi": True
        }


@dataclass(frozen=True)
class DataValidation:
    range: GridRange
    rule: DropDownValidationRule | None
    
    @classmethod
    def from_json(cls, data: dict[str, Any]):
        return cls(
            range=GridRange.from_json(data['range']),
            rule=DropDownValidationRule.from_json(data['rule']) if data['rule'] else None
        )
        
    def to_json(self):
        return {
            "range": self.range.to_json(),
            "rule": self.rule.to_json() if self.rule is not None else {},
            "filteredRowsIncluded": True
        }
        
# ===================================================================================================

class Sheet:
    def __init__(self, jsonsheet: dict[str, Any]):
        self._jsonsheet: dict[str, Any] = jsonsheet
        
    @classmethod
    def from_json(cls, jsonsheet: dict[str, Any]) -> "Sheet":
        return cls(jsonsheet=jsonsheet)
        
    @property
    def id(self) -> int:
        return self._jsonsheet["properties"]["sheetId"]
    
    @property
    def title(self) -> str:
        return self._jsonsheet["properties"]["title"]
    
    @title.setter
    def title(self, value) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("title must be a non-empty string")
        self._jsonsheet["properties"]["title"] = value
        
    def apply_data_validation(self, validation: DataValidation):
        raise NotImplementedError()
    
    def add_protected_range(self, protected_range: ProtectedRange):
        # check if sheet_ids match!
        raise NotImplementedError()
    
    def remove_protected_range(self, protected_range: ProtectedRange):
        # check if sheet_ids match!
        raise NotImplementedError()
    
    def update_protected_range(self, protected_range: ProtectedRange):
        # check if sheet_ids match!
        raise NotImplementedError()
    
    # TODO integrate values via Fetched objects and also introduce stale flag for copy&pasting into the sheet. Do this on spreadsheet level?
    # TODO when stale, no changes should be possible to a sheet
    # TODO get functions 

    # TODO developerMetadata jsonsheet['developerMetadata'] (array)
    # TODO values?!
    
class SheetSnapshot:
    meta_data: Sheet
    values: dict[str, Any] # always prioritize formula
    
    def push(self):
        raise NotImplementedError()

class Spreadsheet:
    
    def __init__(self, id: str, url: str, propertiesjson: dict[str, Any]) -> None:
        self._id: str = id
        self._url: str = url
        self._propertiesjson: dict[str, Any] = propertiesjson
        # TODO load data via values endpoint
        
    @classmethod
    def from_json(cls, jsonspreadsheet: dict[str, Any]) -> "Spreadsheet":
        return cls(
            id=jsonspreadsheet["spreadsheetId"], 
            url=jsonspreadsheet["spreadsheetUrl"],
            propertiesjson=jsonspreadsheet["properties"]
        )
    
    @property
    def id(self) -> str:
        return self._id
    
    @property
    def title(self) -> str:
        return self._propertiesjson["properties"]["title"]
    
    @property
    def url(self) -> str:
        return self._url
    
    @title.setter
    def title(self, value) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("title must be a non-empty string")
        self._propertiesjson["properties"]["title"] = value
        
    def update_request(self):
        requests = []
        
        # properties
        requests.append({
            "updateSpreadsheetProperties": {
                "properties": self._propertiesjson,
                "fields": "*"
            }
        })
        
        # duplicate sheets
        
        # remove sheets
        
        # update sheets if dirty
        
        return requests
        
    def duplicate_sheet(self, sheet_id: int):
        raise NotImplementedError()
    
    def remove_sheet(self, sheet_id: int):
        raise NotImplementedError()

    