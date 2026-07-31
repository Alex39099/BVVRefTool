import copy
import json
import re
from dataclasses import dataclass
from typing import Any


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
        # strip sheet_title if present
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
        
    def to_a1_notation(self, sheet_title: str | None = None) -> str:
        def _idx_to_col(idx: int) -> str:
            result = ""
            while True:
                result = chr(ord("A") + idx % 26) + result
                idx = idx // 26 - 1
                if idx < 0:
                    break
            return result
        start_col = _idx_to_col(self.start_column_idx) if self.start_column_idx is not None else ""
        end_col = _idx_to_col(self.end_column_idx) if self.end_column_idx is not None else ""
        start_row = str(self.start_row_idx + 1) if self.start_row_idx is not None else ""
        end_row = str(self.end_row_idx + 1) if self.end_row_idx is not None else ""

        range_str = f"{start_col}{start_row}:{end_col}{end_row}"
        return f"'{sheet_title}'!{range_str}" if sheet_title else range_str
        
    
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
        
    def copy(self) -> "ProtectedRange":
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

CellValue = str | int | float | bool | None

@dataclass(frozen=True)
class ValueRange:
    range_name: str  # a1 notation
    values: list[list[CellValue]]

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ValueRange":
        return cls(
            range_name=data["range"],
            values=data.get("values", [])
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "range": self.range_name,
            "majorDimension": "ROWS",
            "values": self.values
        }

class FetchedRange:
    _grid_range: GridRange
    _snapshot: list[list[CellValue]] # immutable — fetched from cloud
    current: list[list[CellValue]] # mutable — local changes
    
    def __init__(self, grid_range: GridRange, snapshot: list[list[CellValue]], current: list[list[CellValue]]) -> None:
        self._grid_range = grid_range
        self._snapshot = copy.deepcopy(snapshot)
        self.current = copy.deepcopy(current)
        
    @property
    def grid_range(self) -> GridRange:
        return self._grid_range
    
    @property
    def snapshot(self) -> list[list[CellValue]]:
        return copy.deepcopy(self._snapshot)

    @classmethod
    def from_value_range(cls, value_range: ValueRange, sheet_id: int) -> "FetchedRange":
        snapshot = copy.deepcopy(value_range.values)
        current = copy.deepcopy(value_range.values)
        return cls(
            grid_range=GridRange.from_a1_notation(sheet_id=sheet_id, range_name=value_range.range_name),
            snapshot=snapshot,
            current=current
        )

    @property
    def is_dirty(self) -> bool:
        return self.current != self.snapshot

    def mark_clean(self) -> None:
        self._snapshot = copy.deepcopy(self.current)
        
    def current_value_range(self, sheet_title: str) -> ValueRange:
        return ValueRange(
            range_name=self.grid_range.to_a1_notation(sheet_title=sheet_title),
            values=self.current
        )
    
class Sheet:
    
    # TODO add properties for dimensions
    
    def __init__(self, spreadsheet: "Spreadsheet", jsonsheet: dict[str, Any]):
        self.spreadsheet = spreadsheet
        self._jsonsheet: dict[str, Any] = copy.deepcopy(jsonsheet)
        self._is_dirty = False
        self.stale = False
        
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
        self._is_dirty = True
    
    def apply_data_validation(self, validation: DataValidation):
        raise NotImplementedError()
    
    @property
    def protected_ranges(self) -> tuple[ProtectedRange, ...]:
        return self.spreadsheet.protected_ranges[self.id]
    
    def add_protected_range(self, protected_range: ProtectedRange):
        # check if sheet_ids match
        if self.id != protected_range.range.sheet_id:
            raise ValueError(f"sheet_id do not match: self = {self.id} vs. protectedRange = {protected_range.range.sheet_id}")
        self.spreadsheet.add_protected_range(protected_range)
    
    def remove_protected_range(self, protected_range: ProtectedRange):
        # check if sheet_ids match
        if self.id != protected_range.range.sheet_id:
            raise ValueError(f"sheet_id do not match: self = {self.id} vs. protectedRange = {protected_range.range.sheet_id}")
        if protected_range.id is None:
            raise ValueError("cannot remove protected range without id (not synced yet)")
        self.spreadsheet.remove_protected_range(protected_range_id=protected_range.id)
    
    def get_values(self, grid_range: GridRange | None = None):
        # get all values immediately
        raise NotImplementedError()
    
    def set_values(self, value_range: ValueRange):
        # not possible when staled
        # if sheet_title is present, should match actual titel
        raise NotImplementedError()
    
    def copy_paste(self, source: GridRange, destination: GridRange):
        # queue a copy paste request
        # make instance stale to prevent further changes until pushed
        raise NotImplementedError()
    
    @property
    def is_dirty(self) -> bool:
        return self._is_dirty
    
    def mark_clean(self):
        self._is_dirty = False
        self.stale = False
        
    
    
    
    # TODO integrate values via Fetched objects and also introduce stale flag for copy&pasting into the sheet. Do this on spreadsheet level?
    # TODO when stale, no changes should be possible to a sheet
    # TODO get functions 

    # TODO developerMetadata jsonsheet['developerMetadata'] (array) (ONLY WHEN INCLUDING VALUES! Otherwise use metadata endpoint)
    # TODO values?!

class Spreadsheet:
    
    _sheets: list[Sheet]
    _original_sheet_order: list[int]
    
    _protected_ranges: dict[int, set[ProtectedRange]]  # added ranges have id = None
    _removing_protected_range_ids: set[int]
    
    # TODO developerMetadata
    
    def __init__(self, id: str, url: str, jsonobject: dict[str, Any]) -> None:
        self._id: str = id
        self._url: str = url
        self._propertiesjson: dict[str, Any] = copy.deepcopy(jsonobject['properties'])
        
        self._sheets: list[Sheet] = []
        self._protected_ranges: dict[int, set[ProtectedRange]] = {}
        for sheet_json in jsonobject['sheets']:
            sheet_id = sheet_json['properties']['sheetId']
            protected_ranges = {ProtectedRange.from_json(d) for d in sheet_json.get('protectedRanges', {})}
            self._protected_ranges[sheet_id] = protected_ranges
        self._removing_protected_range_ids: set[int] = set()
        self._original_sheet_order: list[int] = [s.id for s in self._sheets]

        # TODO load data via values endpoint
        
    @property
    def sheets(self) -> tuple[Sheet, ...]:
        return tuple(self._sheets)
    
    def get_sheet_by_id(self, sheet_id: int) -> Sheet:
        for sheet in self._sheets:
            if sheet.id == sheet_id:
                return sheet
        raise ValueError(f"no sheet with id {sheet_id}")
    
    def reorder_sheets(self, sheet_ids: list[int]) -> None:
        current_ids = [s.id for s in self._sheets]
        if sorted(sheet_ids) != sorted(current_ids):
            raise ValueError("sheet_ids must contain exactly the same ids as the current sheets")
        self._sheets = [self.get_sheet_by_id(sid) for sid in sheet_ids]
        
    @property
    def protected_ranges(self) -> dict[int, tuple[ProtectedRange, ...]]:
        return {k: tuple(self._protected_ranges[k]) for k in self._protected_ranges}
        
    def add_protected_range(self, protected_range: ProtectedRange):
        existing_ids = {pr.id for prs in self._protected_ranges.values() for pr in prs if pr.id is not None}
        if protected_range.id in existing_ids:
            raise ValueError("id already taken")
        self._protected_ranges.setdefault(protected_range.range.sheet_id, set()).add(protected_range)
        
    def remove_protected_range(self, protected_range_id: int):
        for pr_list in self._protected_ranges.values():
            for pr in pr_list:
                if protected_range_id == pr.id:
                    self._removing_protected_range_ids.add(protected_range_id)
                    pr_list.remove(pr)
                    return
        raise KeyError(f"id {protected_range_id} not found")
    
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
        
    def fetch(self, include_data=False):
        pass
    
    def push(self):
        # update spreadsheet if dirty
        # update sheet properties if dirty
        # update protected ranges (we might want to block changes bc of stale as well here)
        # update developerMetadata
        # make duplicate sheet stuff
        # make copy & paste stuff
        # mark everything as clean
        
        # possibly do for each sheet individually. 
        # The sheets should provide the batch requests and spreadsheet should execute them
        pass
        
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
        self.get_sheet_by_id(sheet_id).stale = True
        raise NotImplementedError()
    
    def remove_sheet(self, sheet_id: int):
        raise NotImplementedError()

    