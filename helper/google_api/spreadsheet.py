from __future__ import annotations

import copy
import json
import re
from collections.abc import Iterator, MutableMapping
from dataclasses import dataclass
from typing import Any


class TrackedModel:
    _WILDCARD_FIELD = "*"
    _dirty_fields: set[str]
    
    def __init__(self, is_dirty: bool = False):
        self._dirty_fields = set()
        if is_dirty:
            self._dirty_fields.add(self._WILDCARD_FIELD)
        
    def _mark_dirty(self, field_name: str):
        self._dirty_fields.add(field_name)
        
    def mark_clean(self):
        self._dirty_fields.clear()
        
    @property
    def is_dirty(self):
        return len(self._dirty_fields) != 0
        
    @property
    def dirty_field_mask(self) -> str:
        if self._WILDCARD_FIELD in self._dirty_fields:
            return self._WILDCARD_FIELD
        field_mask = ",".join(self._dirty_fields)
        return field_mask

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
            
    @property
    def row_count(self) -> int | None:
        """Number of rows in the range. Returns None if the range is unbounded vertically."""
        if self.start_row_idx is None or self.end_row_idx is None:
            return None
        return self.end_row_idx - self.start_row_idx + 1  # +1 because end is inclusive

    @property
    def col_count(self) -> int | None:
        """Number of columns in the range. Returns None if the range is unbounded horizontally."""
        if self.start_column_idx is None or self.end_column_idx is None:
            return None
        return self.end_column_idx - self.start_column_idx + 1  # +1 because end is inclusive
            
    @classmethod
    def from_a1_notation(cls, sheet_id: int, range_name: str) -> GridRange:
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
    def from_json(cls, data: dict[str, int]) -> GridRange:
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
    
    def contains_idx(self, row_idx: int, col_idx: int) -> bool:
        if not isinstance(row_idx, int) or not isinstance(col_idx, int):
            raise TypeError("row_idx and col_idx must be of type int")
        if self.start_row_idx is not None and row_idx < self.start_row_idx:
            return False
        if self.end_row_idx is not None and row_idx > self.end_row_idx:
            return False
        if self.start_column_idx is not None and col_idx < self.start_column_idx:
            return False
        return not (self.end_column_idx is not None and col_idx > self.end_column_idx)
    
    def contains_grid_range(self, other: GridRange) -> bool:
        if self.sheet_id != other.sheet_id:
            return False
        if (self.start_row_idx is not None and other.start_row_idx is not None and
                other.start_row_idx < self.start_row_idx):
            return False
        if (self.end_row_idx is not None and other.end_row_idx is not None and
                other.end_row_idx > self.end_row_idx):
            return False
        if (self.start_column_idx is not None and other.start_column_idx is not None and
                other.start_column_idx < self.start_column_idx):
            return False
        return not (self.end_column_idx is not None and other.end_column_idx is not None and other.end_column_idx > self.end_column_idx)
    
    def contains_a1(self, a1_notation: str) -> bool:
        other = GridRange.from_a1_notation(sheet_id=self.sheet_id, range_name=a1_notation)
        return self.contains_grid_range(other)
    
    def __contains__(self, key: tuple[int, int] | str | GridRange):
        if isinstance(key, GridRange):
            return self.contains_grid_range(key)
        if isinstance(key, tuple):
            return self.contains_idx(key[0], key[1])
        if isinstance(key, str):
            return self.contains_a1(key)
        raise TypeError("key must be of type tuple[int, int] or str")

       
class ProtectedRange(TrackedModel):
    
    _jsonobject: dict[str, Any]
    
    def __init__(self, jsonobject: dict[str, Any] | None = None, range: GridRange | dict[str, int] | None = None, is_dirty: bool = True):
        super().__init__(is_dirty)
        self._jsonobject: dict[str, Any] = {}
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
    def from_json(cls, jsonobject: dict[str, Any], is_dirty: bool = False) -> ProtectedRange:
        return cls(jsonobject, is_dirty = is_dirty)
    
    def to_json(self):
        # return a copy
        return json.loads(json.dumps(self._jsonobject))
        
    def copy(self) -> ProtectedRange:
        json_copy = json.loads(json.dumps(self._jsonobject))
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
        
    def request_json(self) -> dict[str, Any]:
        keys_to_remove = {'namedRangeId', 'tableId'}
        writing_json = {k: v for k, v in self._jsonobject.items() if k not in keys_to_remove}
        return writing_json
    
class SheetDeveloperMetadata(MutableMapping):
    _sheet_id: int
    _metadata_id: int | None
    _data: dict[str, str]
    _snapshot: dict[str, str]
    
    def __init__(self, sheet_id: int, id: int | None = None, data: dict[str, str] | None = None) -> None:
        self._sheet_id = sheet_id
        self._metadata_id = id
        self._data = dict(data or {})
        self._snapshot = dict(data or {})
        
    @classmethod
    def from_json(cls, jsonobject: dict[str, Any]) -> SheetDeveloperMetadata:
        return cls(
            sheet_id=jsonobject['location']['sheetId'],
            id=jsonobject.get('metadataId'),
            data=json.loads(jsonobject['metadataValue'])
        )
    
    def to_json(self):
        data = {
            "metadataKey": type(self).__name__,
            "metadataValue": json.dumps(self._data),
            "location": {"sheetId": self._sheet_id}
        }
        if self._metadata_id is not None:
            data['metadataId'] = self._metadata_id
        return data
    
    def request_json(self):
        keys_to_remove = {'location'}
        writing_json = {k: v for k, v in self.to_json() if k not in keys_to_remove}
        return writing_json
    
    @property
    def is_dirty(self):
        return self._data != self._snapshot
    
    def mark_clean(self):
        self._snapshot = dict(self._data)
        
    def push(self):
        pass
        
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

@dataclass(frozen=True)
class DropDownValidationRule:
    a1_notation_range: str
    input_message: str
    strict: bool
        
    @classmethod
    def from_json(cls, data: dict[str, Any]) -> DropDownValidationRule:
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
    def from_json(cls, data: dict[str, Any]) -> ValueRange:
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
    _sheet: Sheet
    _grid_range: GridRange  # guards set_value
    _snapshot: list[list[CellValue]] # immutable — fetched from cloud
    current: list[list[CellValue]] # mutable — local changes
    
    def __init__(self, sheet: Sheet, grid_range: GridRange, fetched_values: list[list[CellValue]]) -> None:
        self._sheet = sheet
        self._grid_range = grid_range
        if any(v is None for v in (
            grid_range.start_row_idx,
            grid_range.end_row_idx,
            grid_range.start_column_idx,
            grid_range.end_column_idx,
        )):
            raise ValueError("FetchedRange requires a fully bounded GridRange.")
        if self._grid_range.sheet_id != self._sheet.id:
            raise ValueError("sheet.id differs from grid_range.sheet_id")
        self._snapshot = copy.deepcopy(fetched_values)
        self.current = copy.deepcopy(fetched_values)

    @property
    def sheet(self) -> Sheet:
        return self._sheet
    
    @property
    def grid_range(self) -> GridRange:
        return self._grid_range
    
    @property
    def snapshot(self) -> list[list[CellValue]]:
        return copy.deepcopy(self._snapshot)

    @classmethod
    def from_value_range(cls, sheet: Sheet, value_range: ValueRange) -> FetchedRange:
        return cls(
            sheet=sheet,
            grid_range=GridRange.from_a1_notation(sheet_id=sheet.id, range_name=value_range.range_name),
            fetched_values=value_range.values
        )

    @property
    def is_dirty(self) -> bool:
        return self.current != self.snapshot

    def mark_clean(self) -> None:
        self._snapshot = copy.deepcopy(self.current)
        
    def apply_sheet_bounds(self):
        assert self._grid_range.start_row_idx is not None
        assert self._grid_range.start_column_idx is not None
        assert self._grid_range.end_row_idx is not None
        assert self._grid_range.end_column_idx is not None
        
        sheet_row_count = self.sheet.row_count
        sheet_col_count = self.sheet.column_count
        max_row_count = sheet_row_count - self._grid_range.start_row_idx
        max_col_count = sheet_col_count - self._grid_range.start_column_idx
        
        # Clear cells in rows beyond the new row bound
        for r in range(max_row_count, len(self.current)):
            for c in range(len(self.current[r])):
                self.current[r][c] = None

        # Clear cells in columns beyond the new col bound, active rows only
        for row in self.current[:max_row_count]:
            for c in range(max_col_count, len(row)):
                row[c] = None
        
        # Update the grid_range to reflect new bounds
        # This is only used to guard _set value
        self._grid_range = GridRange(
            sheet_id=self.sheet.id,
            start_row_idx=self._grid_range.start_row_idx,
            end_row_idx=min(sheet_row_count - 1, self._grid_range.end_row_idx),
            start_column_idx=self._grid_range.start_column_idx,
            end_column_idx=min(sheet_col_count - 1, self._grid_range.end_column_idx)
        )
        
    def dirty_cells(self) -> list[ValueRange]:
        result = []
        for r, row in enumerate(self.current):
            for c, value in enumerate(row):
                snapshot_val = (
                self._snapshot[r][c]
                if r < len(self._snapshot) and c < len(self._snapshot[r])
                else None
                )
                if value != snapshot_val:
                    assert self._grid_range.start_row_idx is not None
                    assert self._grid_range.start_column_idx is not None
                    abs_row = self._grid_range.start_row_idx + r
                    abs_col = self._grid_range.start_column_idx + c
                    cell_range = GridRange(
                        sheet_id=self._sheet.id,
                        start_row_idx=abs_row,
                        end_row_idx=abs_row,
                        start_column_idx=abs_col,
                        end_column_idx=abs_col,
                    )
                    result.append(ValueRange(
                        range_name=cell_range.to_a1_notation(self._sheet.title),
                        values=[[value]]
                    ))
        return result
        
    def _set_value(self, row: int, col: int, value: CellValue) -> None:
        while len(self.current) <= row:
            self.current.append([])
        while len(self.current[row]) <= col:
            self.current[row].append(None)
        self.current[row][col] = value
        
    def _resolve_key(self, key: tuple[int, int] | str) -> tuple[int, int]:
        if key not in self._grid_range:
            raise IndexError(f"'{key}' is outside the fetched range {self._grid_range.to_a1_notation()}.")
        if isinstance(key, str):
            cell_range = GridRange.from_a1_notation(sheet_id=self._sheet.id, range_name=key)
            row = (cell_range.start_row_idx or 0) - (self._grid_range.start_row_idx or 0)
            col = (cell_range.start_column_idx or 0) - (self._grid_range.start_column_idx or 0)
        else:
            row, col = key
        return row, col
        
    def __getitem__(self, key: tuple[int, int] | str) -> CellValue:
        row, col = self._resolve_key(key)
        try:
            return self.current[row][col]
        except IndexError:
            return None
        
    def __setitem__(self, key: tuple[int, int] | str, value: CellValue) -> None:
        row, col = self._resolve_key(key)
        self._set_value(row, col, value)
        
    def __delitem__(self, key: tuple[int, int] | str):
        row, col = self._resolve_key(key)
        self._set_value(row, col, None)

    def __contains__(self, key: object) -> bool:
        return any(key in row for row in self.current)

    def __iter__(self) -> Iterator:
        return iter(self.current)

    def __len__(self) -> int:
        return len(self.current)
        
# ===================================================================================================
    
class Sheet(TrackedModel):
    def __init__(self, spreadsheet: Spreadsheet, jsonsheet: dict[str, Any], fetched_values: FetchedRange | None = None, is_dirty: bool = False):
        super().__init__(is_dirty)
        self.spreadsheet = spreadsheet
        self._jsonsheet: dict[str, Any] = copy.deepcopy(jsonsheet)
        self.stale: bool = False
        self._initial_row_count: int = self.row_count
        self._initial_column_count: int = self.column_count
        self.fetched_values: FetchedRange | None = fetched_values
        
    def fetch_values(self):
        raw = self.spreadsheet._gspreadsheets_client.values().get(
            spreadsheetId=self.spreadsheet.id,
            range=self.grid_range.to_a1_notation(self.title)
        )
        value_range = ValueRange.from_json(raw)
        self.fetched_values = FetchedRange.from_value_range(sheet=self, value_range=value_range)
        
    def __getitem__(self, key: tuple[int, int] | str) -> CellValue:
        if self.fetched_values is None:
            self.fetch_values()
        assert self.fetched_values is not None
        return self.fetched_values[key]

    def __setitem__(self, key: tuple[int, int] | str, value: CellValue) -> None:
        self.raise_for_stale()
        if self.fetched_values is None:
            self.fetch_values()
        assert self.fetched_values is not None
        self.fetched_values[key] = value

    def __delitem__(self, key: tuple[int, int] | str) -> None:
        self.raise_for_stale()
        if self.fetched_values is None:
            self.fetch_values()
        assert self.fetched_values is not None
        del self.fetched_values[key]
    
    def raise_for_stale(self):
        if self.stale:
            raise ValueError("instance is stale. Sync first.")
        
    def mark_clean(self) -> None:
        super().mark_clean()
        self.stale = False
        
    @property
    def is_value_dirty(self) -> bool:
        return self.fetched_values is not None and self.fetched_values.is_dirty
    
    def mark_value_clean(self) -> None:
        if self.fetched_values is not None:
            self.fetched_values.mark_clean()

    @property
    def id(self) -> int:
        return self._jsonsheet["properties"]["sheetId"]
    
    @property
    def title(self) -> str:
        return self._jsonsheet["properties"]["title"]
    
    @title.setter
    def title(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("title must be a non-empty string")
        for sheet in self.spreadsheet.sheets:
            if sheet.title == value and sheet != self:
                raise ValueError("title already present in spreadsheet")
        self._jsonsheet["properties"]["title"] = value
        self._mark_dirty('properties.title')
        
    @property
    def row_count(self) -> int:
        return self._jsonsheet["properties"]["gridProperties"]["rowCount"]
    
    @row_count.setter
    def row_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("row_count must be an integer")
        self._jsonsheet["properties"]["gridProperties"]["rowCount"] = value
        self._mark_dirty("properties.gridProperties.rowCount")
        self._resize_fetched_range()
    
    @property
    def column_count(self) -> int:
        return self._jsonsheet["properties"]["gridProperties"]["columnCount"]
    
    @column_count.setter
    def column_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("column_count must be an integer")
        self._jsonsheet["properties"]["gridProperties"]["columnCount"] = value
        self._mark_dirty("properties.gridProperties.columnCount")
        self._resize_fetched_range()
        
    def _resize_fetched_range(self) -> None:
        if self.fetched_values is None:
            return
        self.fetched_values.apply_sheet_bounds()
        
    @property
    def grid_range(self):
        return GridRange(
            sheet_id=self.id,
            start_row_idx=0,
            end_row_idx=self.row_count - 1,
            start_column_idx=0,
            end_column_idx=self.column_count - 1
        )
    
    @property
    def frozen_row_count(self) -> int:
        return self._jsonsheet["properties"]["gridProperties"].get('frozenRowCount', 0)
    
    @frozen_row_count.setter
    def frozen_row_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("frozen_row_count must be an integer")
        self._jsonsheet["properties"]["gridProperties"]["frozenRowCount"] = value
        self._mark_dirty("properties.gridProperties.frozenRowCount")
        
    @property
    def frozen_column_count(self) -> int:
        return self._jsonsheet["properties"]["gridProperties"].get('frozenColumnCount', 0)

    @frozen_column_count.setter
    def frozen_column_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("frozen_column_count must be an integer")
        self._jsonsheet["properties"]["gridProperties"]["frozenColumnCount"] = value
        self._mark_dirty("properties.gridProperties.frozenColumnCount")
    
    def apply_data_validation(self, validation: DataValidation):
        raise NotImplementedError()
    
    @property
    def protected_ranges(self) -> tuple[ProtectedRange, ...]:
        return self.spreadsheet.protected_ranges[self.id]
    
    def add_protected_range(self, protected_range: ProtectedRange):
        if self.id != protected_range.range.sheet_id:
            raise ValueError(f"sheet_id do not match: self = {self.id} vs. protectedRange = {protected_range.range.sheet_id}")
        self.spreadsheet.add_protected_range(protected_range)
    
    def remove_protected_range(self, protected_range: ProtectedRange):
        if self.id != protected_range.range.sheet_id:
            raise ValueError(f"sheet_id do not match: self = {self.id} vs. protectedRange = {protected_range.range.sheet_id}")
        if protected_range.id is None:
            raise ValueError("cannot remove protected range without id (not synced yet)")
        self.spreadsheet.remove_protected_range(protected_range_id=protected_range.id)
        
    @property
    def developer_metadata(self) -> SheetDeveloperMetadata:
        return self.spreadsheet._developerMetadata[self.id]
    
    def copy_paste(self, source: GridRange, destination: GridRange):
        self.raise_for_stale()
        # queue a copy paste request
        # make instance stale to prevent further changes until pushed
        raise NotImplementedError()

class Spreadsheet:
    
    _id: str
    _url: str
    _propertiesjson: dict[str, Any]
    
    _sheets: list[Sheet]
    _original_sheet_order: list[int]
    _duplicate_sheet_requests: list[dict[str, Any]]
    
    _protected_ranges: dict[int, set[ProtectedRange]]
    _removing_protected_range_ids: dict[int, set[int]]
    
    _developerMetadata: dict[int, SheetDeveloperMetadata]
    
    def __init__(self, gspreadsheets_client, spreadsheet_id: str) -> None:
        self._gspreadsheets_client = gspreadsheets_client
        fields = "spreadsheetId,spreadsheetUrl,properties,sheets.properties,sheets.protectedRanges,sheets.developerMetadata"
        spreadsheet_json: dict[str, Any] = self._gspreadsheets_client.get(
            spreadsheetId=spreadsheet_id,
            fields=fields
        )
        
        self._id: str = spreadsheet_json['spreadsheetId']
        self._url: str = spreadsheet_json['spreadsheetUrl']
        self._propertiesjson: dict[str, Any] = spreadsheet_json['properties']
        
        self._sheets: list[Sheet] = []
        self._protected_ranges: dict[int, set[ProtectedRange]] = {}
        self._removing_protected_range_ids: dict[int, set[int]] = {}
        self._developerMetadata: dict[int, SheetDeveloperMetadata] = {}
        for sheet_json in spreadsheet_json['sheets']:
            sheet_id = sheet_json['properties']['sheetId']
            self._protected_ranges[sheet_id] = {ProtectedRange.from_json(d) for d in sheet_json.get('protectedRanges', {})}
            self._removing_protected_range_ids[sheet_id] = set()
            sheet_developer_metadata_json = sheet_json.get('developerMetadata', [{}])[0]
            self._developerMetadata[sheet_id] = (
                SheetDeveloperMetadata.from_json(sheet_developer_metadata_json) 
                if sheet_developer_metadata_json else SheetDeveloperMetadata(sheet_id))

        self._original_sheet_order: list[int] = [s.id for s in self._sheets]
        
    # =========================================================================================================
    
    @property
    def id(self) -> str:
        return self._id
    
    @property
    def title(self) -> str:
        return self._propertiesjson["properties"]["title"]
    
    @property
    def url(self) -> str:
        return self._url
    
    # =========================================================================================================
        
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
        
    def duplicate_sheet(self, sheet_id: int, new_sheet_id: int | None, new_sheet_title: str | None):
        self.get_sheet_by_id(sheet_id).stale = True
        # TODO choose a new_sheet_id if None
        # TODO create a duplicate method in Sheet that duplicates the json as well as values if any are loaded
        raise NotImplementedError()
        
    def remove_sheet(self, sheet_id: int):
        sheet = self.get_sheet_by_id(sheet_id)
        sheet.raise_for_stale()
        self._original_sheet_order.remove(sheet_id)
        self._sheets.remove(sheet)
        del self._protected_ranges[sheet_id]
        # TODO queue delete request?
        
    # =========================================================================================================
        
    @property
    def protected_ranges(self) -> dict[int, tuple[ProtectedRange, ...]]:
        return {k: tuple(self._protected_ranges[k]) for k in self._protected_ranges}
        
    def add_protected_range(self, protected_range: ProtectedRange):
        existing_ids = {pr.id for prs in self._protected_ranges.values() for pr in prs if pr.id is not None}
        if protected_range.id in existing_ids:
            raise ValueError("id already taken")
        if protected_range.range.sheet_id not in self._protected_ranges:
            raise ValueError("sheet of this protected_range is not part of this spreadsheet")
        self._protected_ranges[protected_range.range.sheet_id].add(protected_range)
        
    def remove_protected_range(self, protected_range_id: int):
        for sheet_id, protected_ranges in self._protected_ranges.items():
            for protected_range in protected_ranges:
                if protected_range_id == protected_range.id:
                    self._removing_protected_range_ids[sheet_id].add(protected_range_id)
                    protected_ranges.remove(protected_range)
                    return
        raise KeyError(f"id {protected_range_id} not found")
    
    # =========================================================================================================
    
    def sheet_developer_metadata(self) -> dict[int, SheetDeveloperMetadata]:
        return dict(self._developerMetadata)
    
    # =========================================================================================================
        
    def fetch(self):
        self.__init__(self._gspreadsheets_client, self.id)
    
    def push(self):
        batch_updates: dict[int, list[dict[str, Any]]] = {}
        
        for sheet in self._sheets:
            sheet_batch_updates: list[dict[str, Any]] = []
            if sheet.is_dirty:
                # TODO wo soll die Verantwortlichkeit für die requests sein? Bei den Objekten selbst oder hier?
                pass
                
        
        
        # update sheet properties if dirty
        # update protected ranges
        # update developerMetadata
        # make duplicate sheet stuff
        # make copy & paste stuff
        # mark everything as clean
        
        # possibly do for each sheet individually. 
        # The sheets should provide the batch requests and spreadsheet should execute them
    
    def _batch_update(self, requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not requests:
            return []
        response = self._gspreadsheets_client.batchUpdate(
            spreadsheetId=self.id,
            body={
                "requests": requests
            }
        )
        return response
        
    def _batch_values_update(self, requests: list[dict[str, Any]]):
        raise NotImplementedError()
    
    # TODO was noch zu tun ist: copy&paste und duplicate requests. Objekte updaten vs überschreiben
    