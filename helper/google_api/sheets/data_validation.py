from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from helper.google_api.sheets.grid_range import GridRange
from helper.google_api.sheets.sheet import Sheet


@dataclass(frozen=True)
class DropDownValidationRule:
    sheet: Sheet
    range: GridRange
    input_message: str
    strict: bool
    
    def __post_init__(self):
        if self.sheet.id != self.range.sheet_id:
            raise ValueError("range does not belong to given sheet")
    
    def to_json(self) -> dict[str, Any]:
        bound_range = GridRange(
            sheet_id=self.sheet.id,
            start_row_idx=self.range.start_row_idx or 0,
            end_row_idx=min(self.sheet.row_count - 1, self.range.end_row_idx or self.sheet.row_count - 1),
            start_column_idx=self.range.start_column_idx or 0,
            end_column_idx=min(self.sheet.column_count - 1, self.range.end_column_idx or self.sheet.column_count - 1)
        )
        a1_notation_range = bound_range.to_a1_notation(sheet_title=self.sheet.title,fixed=True)
        return {
            "condition": {
                "type": "ONE_OF_RANGE",
                "values": [{
                    "userEnteredValue": a1_notation_range
                }]
            },
            "inputMessage": self.input_message,
            "strict": self.strict,
            "showCustomUi": True
        }


@dataclass(frozen=True)
class DataValidation:
    sheet: Sheet
    range: GridRange
    rule: DropDownValidationRule | None
    
    def to_json(self):
        return {
            "range": self.range.to_json(),
            "rule": self.rule.to_json() if self.rule is not None else {},
            "filteredRowsIncluded": True
        }
