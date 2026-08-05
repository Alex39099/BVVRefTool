from __future__ import annotations

import copy
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from helper.google_api.grid_range import GridRange
from helper.google_api.sheet import Sheet

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
        """ Constructs a FetchedRange.

        Args:
            sheet (Sheet): the sheet this FetchedRange belongs to
            grid_range (GridRange): a fully bound GridRange of the given sheet
            fetched_values (list[list[CellValue]]): the fetched values

        Raises:
            ValueError: if the gridRange is not bound in every direction
        """
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
        """ Create a FetchedRange from ValueRange

        Args:
            sheet (Sheet): the sheet from which the values were taken
            value_range (ValueRange): the values

        Returns:
            FetchedRange: instance of FetchedRange
        """
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
        """ Applies the current sheet bounds to the underlying GridRange """
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
    
    @property
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
        self._sheet.raise_for_stale()
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
