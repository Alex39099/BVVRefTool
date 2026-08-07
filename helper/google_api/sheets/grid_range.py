from __future__ import annotations

import re
from dataclasses import dataclass


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
        
    def to_a1_notation(self, sheet_title: str | None = None, fixed: bool = False) -> str:
        """ Constructs the A1 notation of this GridRange

        Args:
            sheet_title (str | None, optional): sheet_title. Defaults to None (first sheet of spreadsheet)

        Returns:
            str: _description_
        """
        def _idx_to_col(idx: int) -> str:
            result = ""
            while True:
                result = chr(ord("A") + idx % 26) + result
                idx = idx // 26 - 1
                if idx < 0:
                    break
            return result
        fix = "$" if fixed else ""
        start_col = f"{fix}{_idx_to_col(self.start_column_idx)}" if self.start_column_idx is not None else ""
        end_col = f"{fix}{_idx_to_col(self.end_column_idx)}" if self.end_column_idx is not None else ""
        start_row = f"{fix}{self.start_row_idx + 1}" if self.start_row_idx is not None else ""
        end_row = f"{fix}{self.end_row_idx + 1}" if self.end_row_idx is not None else ""

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
        """ Returns the json representation of this gridRange. None values are not included.

        Returns:
            dict[str, int]: json representation of this gridRange without None values.
        """
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
        """ Check if the given index is within this gridRange

        Args:
            row_idx (int): row index
            col_idx (int): column index

        Returns:
            bool: True if the indices are both within self
        """
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
        """ Check if the given gridRange is fully within this gridRange

        Args:
            other (GridRange): the other gridRange

        Returns:
            bool: True if other is fully within self
        """
        if self.sheet_id != other.sheet_id:
            return False
        
        if (self.start_row_idx or 0) > (other.start_row_idx or 0):
            return False
        if self.end_row_idx is not None and other.end_row_idx is None:
            return False
        if self.end_row_idx is not None and other.end_row_idx is not None and self.end_row_idx < other.end_row_idx:
            return False
        if (self.start_column_idx or 0) > (other.start_column_idx or 0):
            return False
        if self.end_column_idx is not None and other.end_column_idx is None:
            return False
        return not (self.end_column_idx is not None and other.end_column_idx is not None and self.end_column_idx < other.end_column_idx)
    
    def contains_a1(self, a1_notation: str) -> bool:
        """ Check if the given a1_notation is within this gridRange

        Args:
            a1_notation (str): the a1 notation

        Returns:
            bool: True if the given range is fully within self
        """
        other = GridRange.from_a1_notation(sheet_id=self.sheet_id, range_name=a1_notation)
        return self.contains_grid_range(other)
    
    def __contains__(self, key: tuple[int, int] | str | GridRange):
        if isinstance(key, GridRange):
            return self.contains_grid_range(key)
        if isinstance(key, tuple):
            return self.contains_idx(key[0], key[1])
        if isinstance(key, str):
            return self.contains_a1(key)
        raise TypeError("key must be of type tuple[int, int], str in a1 notation or GridRange")
    
    def overlaps(self, other: GridRange) -> bool:
        """ Check if the given gridRange overlaps with this gridRange

        Args:
            other (GridRange): the other gridRange

        Returns:
            bool: True if self and other overlap
        """
        if self.sheet_id != other.sheet_id:
            return False
        
        def overlaps_1d(a_start: int | None, a_end: int | None, b_start: int | None, b_end: int | None) -> bool:
            # None means unbounded — treat as 0 for start, infinity for end
            a_s = a_start if a_start is not None else 0
            b_s = b_start if b_start is not None else 0
            a_e = a_end if a_end is not None else float("inf")
            b_e = b_end if b_end is not None else float("inf")
            return a_s <= b_e and b_s <= a_e

        return (
            overlaps_1d(self.start_row_idx, self.end_row_idx, other.start_row_idx, other.end_row_idx) and
            overlaps_1d(self.start_column_idx, self.end_column_idx, other.start_column_idx, other.end_column_idx)
        )
 